## Docker quick start

1) Copy env example and set your secrets

```bash
copy example.env .env
# edit .env and set BOT_TOKEN, BOT_USERNAME, WEBHOOK_URL if using webhook
```

2) Build and run with Docker Compose

```bash
docker compose build
docker compose up -d
```

- Polling mode: set `USE_WEBHOOK=false` (default). No inbound ports required.
- Webhook mode: set `USE_WEBHOOK=true` and configure `WEBHOOK_URL` to your public HTTPS endpoint. Port `8080` is exposed by the container; publish it or route via a reverse proxy.

3) Logs

```bash
docker compose logs -f
```

4) Updating

```bash
docker compose pull || echo "local build only"
docker compose build --no-cache
docker compose up -d
```

### Notes
- The file `data/agents.csv` is mounted read-only into the container.
- `data/cache.json` is mounted for persistence between restarts.
- Requirements: Docker Engine 20+, Docker Compose v2.
