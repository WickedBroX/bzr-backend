# Persistent Store Postgres Bootstrap

This directory helps spin up a Postgres instance for the persistent transfer store, either locally or on a server.

## Quick Start (Local Development)
```bash
cd bzr-backend/deploy/postgres
cp .env.example .env   # edit credentials
docker compose up -d
```

The database exposes port 5432 by default. Update the backend `.env` with:
```env
TRANSFERS_DATABASE_URL=postgres://bzr:super-secret-password@localhost:5432/bzr_transfers
TRANSFERS_DATA_SOURCE=store
```

## Production Checklist
1. Use a managed Postgres service when possible, or copy this Compose setup to the server (restrict access to trusted hosts).
2. Replace credentials with strong secrets and store them in `/var/www/bzr-backend/.env`.
3. Enable backups/monitoring for the database and ensure disk growth is tracked.
4. After provisioning, restart the services:
   ```bash
   systemctl restart bzr-backend bzr-ingester
   journalctl -u bzr-ingester -n 200 --no-pager
   ```

Once the ingester connects successfully, `/api/health` will populate `chains[]` and report `status: "ok"` with `meta.ready = true`.
