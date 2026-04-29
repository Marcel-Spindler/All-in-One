# Cloud-Deployment - Schnellreferenz

## Was liegt hier drin?

| Datei | Zweck |
|---|---|
| `ANLEITUNG-CLOUD.txt` | **Volle Klick-fuer-Klick-Anleitung** (60 Min Setup) |
| `docker-compose.yml` | Orchestriert alle 4 Tools + Caddy + oauth2-proxy |
| `Caddyfile` | Reverse Proxy mit Auto-HTTPS + Google-Login-Wand |
| `Dockerfile.incident` | Container fuer Incident Tool |
| `Dockerfile.pdl` | Container fuer PDL Fast |
| `Dockerfile.waagen` | Container fuer Waagen Performance |
| `Dockerfile.pscopilot` | Container fuer PS Copilot |
| `cockpit/index.html` | Startseite mit 4 Tool-Buttons |
| `setup-vm.sh` | Einmal-Skript auf der VM (Docker installieren) |
| `.env.example` | Vorlage fuer Geheimnisse |

## Architektur

```
              Internet
                 |
            HTTPS (Lets Encrypt)
                 |
             [Caddy :443]
                 |
        +--------+--------+
        |                 |
   /oauth2/*         alles andere
        |                 |
  [oauth2-proxy]   forward_auth -> nur @hellofresh.de
                          |
        +-----+-----+-----+-----+
        |     |     |     |
        v     v     v     v
   incident  pdl  waagen  ps-copilot
   :8501  :8502  :8505    :3020
```

## Login-Flow

1. Mitarbeiter oeffnet `https://tools.hellofresh-internal.de`
2. Caddy fragt oauth2-proxy: ist eingeloggt?
3. Nein -> Redirect zu Google-Login
4. Google prueft: ist `@hellofresh.de`-Account?
5. Ja -> Cookie gesetzt -> Cockpit erscheint
6. Klick auf Tool -> Caddy leitet weiter (Cookie reicht)

## Datenpersistenz

Volume `app_data` (auf der VM unter `/var/lib/docker/volumes/cloud_app_data/_data/`)
enthaelt alle Tool-Daten. Backup-Skript:

```bash
sudo tar czf /backups/aio-$(date +%F).tar.gz /var/lib/docker/volumes/cloud_app_data
```

Empfohlen: GCP Snapshot-Schedule auf der VM einrichten.
