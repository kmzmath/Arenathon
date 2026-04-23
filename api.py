#!/usr/bin/env python3
"""
Arena Duo Championship API.

Serve os dados coletados por `arena_duo_program1.py` para o front-end do
campeonato e dispara o coletor periodicamente via APScheduler.

Endpoints:
    GET  /health                    -> status do serviço e do coletor
    GET  /scoreboard                -> ranking ordenado (sem partidas)
    GET  /duos/{duo_id}             -> dupla + todas as partidas contabilizadas
    GET  /duos/{duo_id}/matches     -> só as partidas da dupla
    POST /refresh                   -> dispara coleta imediata (opcional token)

Config via variáveis de ambiente:
    RIOT_API_KEY                   (obrigatória)
    ARENA_DUMP_DIR                 default: ./arena_program1_dump
    ARENA_DUOS_XLSX                default: ./duplas.xlsx
    ARENA_PHOTOS_JSON              default: ./players_photos.json
    ARENA_COLLECTOR_SCRIPT         default: ./arena_duo_program1.py
    ARENA_REFRESH_MINUTES          default: 5
    ARENA_REFRESH_ON_STARTUP       default: 1   (0 para desligar)
    ARENA_COLLECTOR_NO_TIMELINE    default: 0   (1 para economizar API calls)
    ARENA_COLLECTOR_TIMEOUT        default: 600
    ARENA_CORS_ORIGINS             default: *   (separado por vírgula)
    ARENA_ADMIN_TOKEN              default: vazio (sem proteção em /refresh)

Executar:
    uvicorn api:app --host 0.0.0.0 --port 8000
"""
from __future__ import annotations

import csv
import json
import logging
import os
import subprocess
import sys
import threading
import time
from contextlib import asynccontextmanager
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from zoneinfo import ZoneInfo

from apscheduler.schedulers.background import BackgroundScheduler
from fastapi import FastAPI, Header, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field


# --------------------------------------------------------------------------
# Configuração
# --------------------------------------------------------------------------
TZ = ZoneInfo("America/Sao_Paulo")

DUMP_DIR = Path(os.getenv("ARENA_DUMP_DIR", "./arena_program1_dump")).resolve()
DUOS_XLSX = Path(os.getenv("ARENA_DUOS_XLSX", "./duplas.xlsx")).resolve()
PHOTOS_JSON = Path(os.getenv("ARENA_PHOTOS_JSON", "./players_photos.json")).resolve()
COLLECTOR_SCRIPT = Path(os.getenv("ARENA_COLLECTOR_SCRIPT", "./arena_duo_program1.py")).resolve()

API_KEY = os.getenv("RIOT_API_KEY", "")
REFRESH_MINUTES = max(1, int(os.getenv("ARENA_REFRESH_MINUTES", "5")))
REFRESH_ON_STARTUP = os.getenv("ARENA_REFRESH_ON_STARTUP", "1") == "1"
COLLECTOR_TIMEOUT_SECONDS = int(os.getenv("ARENA_COLLECTOR_TIMEOUT", "600"))
COLLECTOR_NO_TIMELINE = os.getenv("ARENA_COLLECTOR_NO_TIMELINE", "0") == "1"
CORS_ORIGINS = [o.strip() for o in os.getenv("ARENA_CORS_ORIGINS", "*").split(",") if o.strip()]
ADMIN_TOKEN = os.getenv("ARENA_ADMIN_TOKEN", "")


logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
log = logging.getLogger("arena_api")


# --------------------------------------------------------------------------
# Estado do coletor (em memória)
# --------------------------------------------------------------------------
_collection_lock = threading.Lock()
_state: Dict[str, Any] = {
    "last_success_at": None,
    "last_attempt_at": None,
    "last_error": None,
    "last_duration_seconds": None,
    "running": False,
    "runs_total": 0,
    "runs_failed": 0,
    "last_exit_code": None,
    "last_download_errors": None,
}


def _run_collector() -> None:
    """Executa o coletor como subprocess. Acesso serializado por lock."""
    if not _collection_lock.acquire(blocking=False):
        log.info("Coletor já está rodando; pulando esta execução.")
        return

    _state["running"] = True
    _state["last_attempt_at"] = datetime.now(TZ).isoformat()
    _state["runs_total"] += 1
    started = time.time()

    try:
        if not API_KEY:
            raise RuntimeError("RIOT_API_KEY não definida no ambiente.")
        if not COLLECTOR_SCRIPT.exists():
            raise FileNotFoundError(f"Script do coletor não encontrado: {COLLECTOR_SCRIPT}")
        if not DUOS_XLSX.exists():
            raise FileNotFoundError(f"Planilha de duplas não encontrada: {DUOS_XLSX}")

        cmd = [
            sys.executable,
            str(COLLECTOR_SCRIPT),
            "--duos-xlsx", str(DUOS_XLSX),
            "--api-key", API_KEY,
            "--output", str(DUMP_DIR),
        ]
        if COLLECTOR_NO_TIMELINE:
            cmd.append("--no-timeline")

        # Não logamos a api-key
        safe_cmd = [c if c != API_KEY else "***" for c in cmd]
        log.info("Rodando coletor: %s", " ".join(safe_cmd))

        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=COLLECTOR_TIMEOUT_SECONDS,
        )
        _state["last_exit_code"] = result.returncode

        # O coletor retorna 0 em sucesso pleno e 1 quando houve erros de download
        # mas ainda escreveu os arquivos de saída. Tratamos ambos como "rodou".
        if result.returncode not in (0, 1):
            tail = (result.stderr or "").strip().splitlines()[-15:]
            raise RuntimeError(
                f"Coletor saiu com código {result.returncode}. Últimas linhas:\n"
                + "\n".join(tail)
            )

        # Lê o manifest para expor algumas métricas
        manifest_path = DUMP_DIR / "run_manifest.json"
        if manifest_path.exists():
            try:
                manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
                _state["last_download_errors"] = manifest.get("download_errors_count")
            except Exception:
                _state["last_download_errors"] = None

        _state["last_success_at"] = datetime.now(TZ).isoformat()
        _state["last_error"] = None
        if result.returncode == 1:
            log.warning("Coletor terminou com erros de download parciais.")

    except subprocess.TimeoutExpired:
        _state["runs_failed"] += 1
        _state["last_error"] = f"Timeout após {COLLECTOR_TIMEOUT_SECONDS}s"
        log.exception("Timeout no coletor")
    except Exception as exc:
        _state["runs_failed"] += 1
        _state["last_error"] = str(exc)
        log.exception("Falha na execução do coletor")
    finally:
        _state["last_duration_seconds"] = round(time.time() - started, 2)
        _state["running"] = False
        _collection_lock.release()


# --------------------------------------------------------------------------
# Leitura dos artefatos gerados pelo coletor
# --------------------------------------------------------------------------
def _read_json_with_retry(path: Path, retries: int = 3, delay: float = 0.2) -> Any:
    """Lê um JSON tolerando raras colisões com a escrita do coletor."""
    last_exc: Optional[BaseException] = None
    for _ in range(retries):
        try:
            return json.loads(path.read_text(encoding="utf-8"))
        except (FileNotFoundError, json.JSONDecodeError) as exc:
            last_exc = exc
            time.sleep(delay)
    assert last_exc is not None
    raise last_exc


def _load_players_index() -> Dict[str, Dict[str, Any]]:
    path = DUMP_DIR / "players_resolved.json"
    if not path.exists():
        return {}
    data = _read_json_with_retry(path)
    return {p["riot_id"]: p for p in data.get("players", [])}


def _load_photos_map() -> Dict[str, str]:
    if not PHOTOS_JSON.exists():
        return {}
    try:
        return json.loads(PHOTOS_JSON.read_text(encoding="utf-8"))
    except Exception:
        log.exception("Falha ao ler %s", PHOTOS_JSON)
        return {}


def _load_valid_matches() -> Dict[str, Any]:
    path = DUMP_DIR / "valid_matches.json"
    if not path.exists():
        return {"window": None, "count": 0, "rows": []}
    return _read_json_with_retry(path)


def _load_resolved_duos() -> List[Dict[str, Any]]:
    """Lista de duplas resolvidas (com PUUID etc), independentemente de terem
    partidas válidas. É a fonte de verdade da composição do torneio."""
    path = DUMP_DIR / "duos_resolved.json"
    if not path.exists():
        return []
    data = _read_json_with_retry(path)
    return data.get("duos", [])


def _load_scoreboard_csv() -> List[Dict[str, Any]]:
    path = DUMP_DIR / "scoreboard.csv"
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8", newline="") as fp:
        return list(csv.DictReader(fp))


# --------------------------------------------------------------------------
# Schemas de resposta
# --------------------------------------------------------------------------
class PlayerInfo(BaseModel):
    riot_id: str
    game_name: str
    tag_line: str
    photo_url: Optional[str] = None


class PlayerMatchStats(BaseModel):
    riot_id: str
    game_name: Optional[str] = None
    photo_url: Optional[str] = None
    champion_name: Optional[str] = None
    champion_id: Optional[int] = None
    kills: Optional[int] = None
    deaths: Optional[int] = None
    assists: Optional[int] = None
    gold_earned: Optional[int] = None
    total_damage_dealt_to_champions: Optional[int] = None
    total_damage_taken: Optional[int] = None
    augments: List[Optional[int]] = Field(default_factory=list)


class MatchSummary(BaseModel):
    match_id: str
    queue_id: Optional[int] = None
    game_mode: Optional[str] = None
    game_version: Optional[str] = None
    game_start_local: Optional[str] = None
    game_end_local: Optional[str] = None
    game_duration_seconds: Optional[float] = None
    subteam_placement: Optional[int] = None
    points: Optional[int] = None
    player1: PlayerMatchStats
    player2: PlayerMatchStats


class DuoSummary(BaseModel):
    rank: int
    duo_id: str
    duo_name: str
    total_points: int
    valid_matches: int
    placements: Dict[str, int]
    player1: PlayerInfo
    player2: PlayerInfo


class DuoDetail(DuoSummary):
    matches: List[MatchSummary]


class CollectorStatus(BaseModel):
    last_success_at: Optional[str] = None
    last_attempt_at: Optional[str] = None
    last_error: Optional[str] = None
    last_duration_seconds: Optional[float] = None
    running: bool = False
    runs_total: int = 0
    runs_failed: int = 0
    last_exit_code: Optional[int] = None
    last_download_errors: Optional[int] = None


class ScoreboardResponse(BaseModel):
    last_updated: Optional[str] = None
    window: Optional[Dict[str, Any]] = None
    collector: CollectorStatus
    duos: List[DuoSummary]


# --------------------------------------------------------------------------
# Construção das respostas
# --------------------------------------------------------------------------
def _split_riot_id(riot_id: str) -> Tuple[str, str]:
    if "#" in riot_id:
        name, tag = riot_id.rsplit("#", 1)
        return name, tag
    return riot_id, ""


def _player_info(riot_id: str, players_idx: Dict[str, Any], photos: Dict[str, str]) -> PlayerInfo:
    info = players_idx.get(riot_id, {})
    default_name, default_tag = _split_riot_id(riot_id)
    return PlayerInfo(
        riot_id=riot_id,
        game_name=info.get("game_name") or default_name,
        tag_line=info.get("tag_line") or default_tag,
        photo_url=photos.get(riot_id),
    )


def _build_match_stats(
    participant: Dict[str, Any], riot_id: str, photos: Dict[str, str]
) -> PlayerMatchStats:
    augments = [
        participant.get("player_augment1"),
        participant.get("player_augment2"),
        participant.get("player_augment3"),
        participant.get("player_augment4"),
    ]
    # Também aceita augment5/6 caso o coletor passe a coletar no futuro
    for extra in ("player_augment5", "player_augment6"):
        if extra in participant and participant.get(extra) is not None:
            augments.append(participant.get(extra))

    return PlayerMatchStats(
        riot_id=riot_id,
        game_name=participant.get("riot_id_game_name"),
        photo_url=photos.get(riot_id),
        champion_name=participant.get("champion_name"),
        champion_id=participant.get("champion_id"),
        kills=participant.get("kills"),
        deaths=participant.get("deaths"),
        assists=participant.get("assists"),
        gold_earned=participant.get("gold_earned"),
        total_damage_dealt_to_champions=participant.get("total_damage_dealt_to_champions"),
        total_damage_taken=participant.get("total_damage_taken"),
        augments=augments,
    )


def _build_match_summary(row: Dict[str, Any], photos: Dict[str, str]) -> MatchSummary:
    return MatchSummary(
        match_id=row["match_id"],
        queue_id=row.get("queue_id"),
        game_mode=row.get("game_mode"),
        game_version=row.get("game_version"),
        game_start_local=row.get("game_start_local"),
        game_end_local=row.get("game_end_local"),
        game_duration_seconds=row.get("game_duration_seconds"),
        subteam_placement=row.get("subteam_placement"),
        points=row.get("points"),
        player1=_build_match_stats(row.get("player1", {}), row["player1_riot_id"], photos),
        player2=_build_match_stats(row.get("player2", {}), row["player2_riot_id"], photos),
    )


def _build_view() -> Tuple[List[DuoSummary], Dict[str, List[MatchSummary]], Optional[Dict[str, Any]]]:
    valid = _load_valid_matches()
    rows = valid.get("rows", [])
    window = valid.get("window")
    scoreboard_csv = _load_scoreboard_csv()
    resolved_duos = _load_resolved_duos()
    players_idx = _load_players_index()
    photos = _load_photos_map()

    # Indexa o scoreboard por duo_id pra fazer left-join. Duplas sem partidas
    # válidas não estarão aqui — usaremos zeros pra elas.
    score_by_id: Dict[str, Dict[str, Any]] = {s["duo_id"]: s for s in scoreboard_csv}

    summaries: List[DuoSummary] = []
    for duo in resolved_duos:
        s = score_by_id.get(duo["duo_id"], {})
        placements = {
            str(k): int(s.get(f"placements_{k}") or 0) for k in range(1, 9)
        }
        summaries.append(DuoSummary(
            rank=0,  # preenchido após ordenação
            duo_id=duo["duo_id"],
            duo_name=duo["duo_name"],
            total_points=int(s.get("total_points") or 0),
            valid_matches=int(s.get("valid_matches") or 0),
            placements=placements,
            player1=_player_info(duo["player1_riot_id"], players_idx, photos),
            player2=_player_info(duo["player2_riot_id"], players_idx, photos),
        ))

    # Ordena: pontos desc, depois 1ºs, 2ºs, e duo_id como desempate estável.
    # Duplas sem partidas (0 pts, 0 placements) caem no fim, ordenadas por duo_id.
    summaries.sort(key=lambda d: (
        -d.total_points,
        -d.placements["1"],
        -d.placements["2"],
        d.duo_id,
    ))
    for i, d in enumerate(summaries, start=1):
        d.rank = i

    by_duo: Dict[str, List[MatchSummary]] = {}
    for row in rows:
        ms = _build_match_summary(row, photos)
        by_duo.setdefault(row["duo_id"], []).append(ms)
    for duo_id in by_duo:
        by_duo[duo_id].sort(key=lambda m: m.game_start_local or "", reverse=True)

    return summaries, by_duo, window


# --------------------------------------------------------------------------
# FastAPI app + scheduler
# --------------------------------------------------------------------------
scheduler = BackgroundScheduler(timezone=str(TZ))


@asynccontextmanager
async def lifespan(_: FastAPI):
    if REFRESH_ON_STARTUP:
        threading.Thread(target=_run_collector, daemon=True).start()

    scheduler.add_job(
        _run_collector,
        trigger="interval",
        minutes=REFRESH_MINUTES,
        jitter=30,
        max_instances=1,
        coalesce=True,
        id="collector_job",
        replace_existing=True,
    )
    scheduler.start()
    log.info("Scheduler iniciado. Refresh a cada %s min (jitter=30s).", REFRESH_MINUTES)

    try:
        yield
    finally:
        scheduler.shutdown(wait=False)


app = FastAPI(title="Arena Duo Championship API", version="1.0.0", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=CORS_ORIGINS or ["*"],
    allow_credentials=False,
    allow_methods=["GET", "POST"],
    allow_headers=["*"],
)


@app.get("/health")
def health() -> Dict[str, Any]:
    return {
        "status": "ok",
        "now": datetime.now(TZ).isoformat(),
        "dump_dir": str(DUMP_DIR),
        "dump_dir_exists": DUMP_DIR.exists(),
        "collector_script_exists": COLLECTOR_SCRIPT.exists(),
        "duos_xlsx_exists": DUOS_XLSX.exists(),
        "photos_json_exists": PHOTOS_JSON.exists(),
        "riot_api_key_set": bool(API_KEY),
        "refresh_minutes": REFRESH_MINUTES,
        "collector": _state,
    }


@app.get("/scoreboard", response_model=ScoreboardResponse)
def scoreboard() -> ScoreboardResponse:
    summaries, _, window = _build_view()
    return ScoreboardResponse(
        last_updated=_state["last_success_at"],
        window=window,
        collector=CollectorStatus(**_state),
        duos=summaries,
    )


@app.get("/duos/{duo_id}", response_model=DuoDetail)
def duo_detail(duo_id: str) -> DuoDetail:
    try:
        summaries, by_duo, _ = _build_view()
    except FileNotFoundError:
        raise HTTPException(status_code=503, detail="Coleta ainda não foi executada.")
    duo = next((d for d in summaries if d.duo_id == duo_id), None)
    if duo is None:
        raise HTTPException(status_code=404, detail=f"duo_id {duo_id} não encontrado.")
    return DuoDetail(**duo.model_dump(), matches=by_duo.get(duo_id, []))


@app.get("/duos/{duo_id}/matches", response_model=List[MatchSummary])
def duo_matches(duo_id: str) -> List[MatchSummary]:
    try:
        _, by_duo, _ = _build_view()
    except FileNotFoundError:
        raise HTTPException(status_code=503, detail="Coleta ainda não foi executada.")
    return by_duo.get(duo_id, [])


@app.post("/refresh")
def refresh_now(x_admin_token: Optional[str] = Header(default=None)) -> Dict[str, Any]:
    if ADMIN_TOKEN and x_admin_token != ADMIN_TOKEN:
        raise HTTPException(status_code=401, detail="Token inválido.")
    if _state["running"]:
        return {"started": False, "reason": "Coletor já está em execução."}
    threading.Thread(target=_run_collector, daemon=True).start()
    return {"started": True}
