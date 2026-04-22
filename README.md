# Arena Duo Championship API

API em FastAPI que serve para o front-end os dados coletados pelo
`arena_duo_program1.py`, atualizando-os sozinha em intervalos regulares.

## Como rodar

```bash
pip install -r requirements.txt

export RIOT_API_KEY="RGAPI-..."
# (opcionais — os defaults batem com o layout atual do repo)
export ARENA_DUMP_DIR="./arena_program1_dump"
export ARENA_DUOS_XLSX="./duplas.xlsx"
export ARENA_PHOTOS_JSON="./players_photos.json"
export ARENA_COLLECTOR_SCRIPT="./arena_duo_program1.py"
export ARENA_REFRESH_MINUTES=5

uvicorn api:app --host 0.0.0.0 --port 8000
```

Ao iniciar, o serviço dispara o coletor uma vez em background e depois o
agenda para rodar a cada `ARENA_REFRESH_MINUTES` minutos (com um jitter
de 30 s para desencontrar de outros jobs).

## Endpoints

### `GET /scoreboard`
Retorna o ranking ordenado com totais por dupla (sem as partidas). Essa é
a rota que alimenta a lista principal do front.

```json
{
  "last_updated": "2026-04-22T10:05:18-03:00",
  "window": {
    "start_local_iso": "2026-04-29T13:00:00-03:00",
    "end_local_iso":   "2026-05-13T23:59:59-03:00"
  },
  "collector": { "running": false, "runs_total": 12, "...": "..." },
  "duos": [
    {
      "rank": 1,
      "duo_id": "1",
      "duo_name": "NOME DA EQUIPE",
      "total_points": 240,
      "valid_matches": 30,
      "placements": {"1": 10, "2": 5, "3": 3, "4": 4, "5": 2, "6": 3, "7": 2, "8": 1},
      "player1": {
        "riot_id": "kmyzth#mafer",
        "game_name": "kmyzth",
        "tag_line": "mafer",
        "photo_url": "https://cdn.seusite.com/players/kmyzth.png"
      },
      "player2": { "...": "..." }
    }
  ]
}
```

### `GET /duos/{duo_id}`
Retorna a dupla + todas as partidas contabilizadas (ordenadas do mais
recente para o mais antigo). É o que o front carrega quando o usuário
expande a linha da dupla.

```json
{
  "rank": 1,
  "duo_id": "1",
  "duo_name": "NOME DA EQUIPE",
  "total_points": 240,
  "valid_matches": 30,
  "placements": { "...": "..." },
  "player1": { "...": "..." },
  "player2": { "...": "..." },
  "matches": [
    {
      "match_id": "BR1_3208239420",
      "queue_id": 1700,
      "game_mode": "CHERRY",
      "game_version": "16.4.746.5697",
      "game_start_local": "2026-04-30T20:13:14-03:00",
      "game_end_local":   "2026-04-30T20:38:07-03:00",
      "game_duration_seconds": 1492.0,
      "subteam_placement": 1,
      "points": 10,
      "player1": {
        "riot_id": "kmyzth#mafer",
        "photo_url": "https://cdn.seusite.com/players/kmyzth.png",
        "champion_name": "Ahri", "champion_id": 103,
        "kills": 23, "deaths": 5, "assists": 19,
        "gold_earned": 12954,
        "total_damage_dealt_to_champions": 54320,
        "total_damage_taken": 28110,
        "augments": [19, 52, 74, 0]
      },
      "player2": { "...": "..." }
    }
  ]
}
```

### `GET /duos/{duo_id}/matches`
Só a lista de partidas da dupla. Útil se o front já tem o resumo em cache.

### `GET /health`
Status do serviço e do coletor (útil para uptime monitors).

### `POST /refresh`
Dispara o coletor fora do agendamento. Se `ARENA_ADMIN_TOKEN` estiver
definido, o header `X-Admin-Token: <token>` passa a ser obrigatório.

## Mapeamento de fotos

As fotos não vêm da Riot API. Mantenha um JSON plano em
`ARENA_PHOTOS_JSON` mapeando `riot_id` → URL. Exemplo em
`players_photos.example.json`.

```json
{
  "kmyzth#mafer": "https://cdn.seusite.com/players/kmyzth.png",
  "Vinas#OVN":    "https://cdn.seusite.com/players/vinas.png"
}
```

Jogador sem entrada → `photo_url = null` (o front mostra avatar default).

## Rate limits da Riot

Chave de desenvolvimento: 20 req/s, 100 req/2 min.

Cada refresh custa aproximadamente, por jogador único:
- 2 chamadas (resolve conta + plataforma) — o coletor **hoje refaz isso
  todo ciclo**. Para 20 jogadores são 40 chamadas desperdiçadas por
  execução. Otimização futura recomendada (ver abaixo).
- 2 chamadas para listar match IDs (uma por queue, 1700 e 1710).
- N chamadas para partidas novas (match + timeline). Já baixadas ficam
  em cache no `raw/` e não são rebaixadas.

Com 20 jogadores e 5 min de intervalo, cada execução custa ~90 chamadas
em regime estável. Cabe tranquilo no limite de 100/2min **se** você
rodar a cada 5 min. Se quiser 2 em 2 min, desligue a timeline
(`ARENA_COLLECTOR_NO_TIMELINE=1`) ou peça uma production key.

## Variáveis de ambiente

| Variável                      | Default                       | Descrição                                    |
|-------------------------------|-------------------------------|----------------------------------------------|
| `RIOT_API_KEY`                | —                             | Obrigatória. Chave da Riot Developer.        |
| `ARENA_DUMP_DIR`              | `./arena_program1_dump`       | Onde o coletor grava as saídas.              |
| `ARENA_DUOS_XLSX`             | `./duplas.xlsx`               | Planilha de duplas.                          |
| `ARENA_PHOTOS_JSON`           | `./players_photos.json`       | Mapa de fotos por `riot_id`.                 |
| `ARENA_COLLECTOR_SCRIPT`      | `./arena_duo_program1.py`     | Caminho do coletor.                          |
| `ARENA_REFRESH_MINUTES`       | `5`                           | Intervalo entre execuções.                   |
| `ARENA_REFRESH_ON_STARTUP`    | `1`                           | Se `1`, dispara coleta ao subir a API.       |
| `ARENA_COLLECTOR_NO_TIMELINE` | `0`                           | Se `1`, não baixa timeline (economiza API).  |
| `ARENA_COLLECTOR_TIMEOUT`     | `600`                         | Timeout (s) de cada execução do coletor.     |
| `ARENA_CORS_ORIGINS`          | `*`                           | Origens permitidas, separadas por vírgula.   |
| `ARENA_ADMIN_TOKEN`           | (vazio)                       | Se setado, protege `POST /refresh`.          |

## Deploy sugerido

Para produção, atrás de um reverse-proxy com HTTPS (Nginx/Caddy) e com
o processo gerenciado por systemd/supervisor. Um único worker do
uvicorn basta — o scheduler precisa rodar em **um** processo só para
não duplicar chamadas à Riot (`--workers 1`).

```ini
# /etc/systemd/system/arena-api.service
[Service]
WorkingDirectory=/opt/arena
Environment=RIOT_API_KEY=RGAPI-...
Environment=ARENA_ADMIN_TOKEN=troque-isto
ExecStart=/opt/arena/.venv/bin/uvicorn api:app --host 127.0.0.1 --port 8000 --workers 1
Restart=always
```

## Próximas melhorias sugeridas

1. **Cachear `players_resolved.json` no coletor** — hoje ele resolve
   PUUID e plataforma de todo mundo a cada execução. Bastaria carregar
   o JSON existente e só chamar a Riot para jogadores ainda não
   resolvidos.
2. **Escrita atômica** no coletor (`save_json` → grava em `.tmp` e
   renomeia). A API já tolera leituras durante escrita com retry, mas
   atomicidade real no escritor elimina a corrida.
3. **Filtrar `endOfGameResult == "GameComplete"`** em
   `evaluate_duo_match` para descartar partidas abortadas.
4. **Cache em memória de 5–15 s** nos endpoints se o tráfego crescer —
   hoje cada request relê os JSONs (rápido para volumes pequenos).
