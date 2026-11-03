# Connection setup (no secrets live in this repo)

This repo intentionally contains **zero real credentials**. Every value
below that is a secret (client secret, token, password) is described by
name and location only — never its value — and must be supplied locally by
whoever runs this next. See [DOCUMENTATION.md](DOCUMENTATION.md) for how
these are used; this file is just the setup checklist.

## 1. `~/.databrickscfg` — CLI / SDK auth profile

Every Python script under `src/`, `setup/`, and the dashboard's backend
authenticates via the `databricks-sdk`'s profile mechanism, reading a
`[dev]` profile from `~/.databrickscfg` (NOT committed — matched by
`.gitignore`'s `.databrickscfg` pattern).

Create it with:

```ini
[dev]
host       = https://adb-7405617014325769.9.azuredatabricks.net
client_id  = <your service principal's Azure AD application (client) ID>
client_secret = <your service principal's Azure AD client secret>
```

This project uses **Azure AD M2M (machine-to-machine) auth** — a service
principal's `client_id`/`client_secret`, not a personal access token. To
create one: Azure Portal → Microsoft Entra ID → App registrations → New
registration → Certificates & secrets → New client secret. Then grant that
service principal access to the Databricks workspace (admin console →
Service principals → Add) and to the `talend_migration` catalog
(`setup/bootstrap_workspace.py --grant-to <the SP's application ID or UUID>`).

Alternatively, for a human/interactive profile instead of M2M:
```bash
databricks auth login --host https://adb-7405617014325769.9.azuredatabricks.net --profile dev
```

Verify it works:
```bash
databricks current-user me --profile dev
```

## 2. `apps/migration-dashboard/migration-dashboard/.env` — dashboard local dev

Used only by `npm run dev` (the local Vite/Express dev server for the
dashboard). The deployed app itself gets these from `app.yaml`'s
`valueFrom` resource bindings instead — this file is strictly a local-dev
convenience and is also gitignored.

```ini
DATABRICKS_CONFIG_PROFILE=dev
DATABRICKS_WAREHOUSE_ID=<SQL warehouse id — see databricks.yml's sql_warehouse_id variable>
DATABRICKS_VOLUME_FILES=<UC volume path, e.g. talend_migration.samples.talend_export>
DATABRICKS_APP_PORT=8000
DATABRICKS_APP_NAME=migration-dashboard
FLASK_RUN_HOST=0.0.0.0
```

None of these six values are secret — they're the same non-sensitive
resource identifiers already committed in `databricks.yml`/`app.yaml`
(workspace host, warehouse id, volume path). This file exists only because
Vite/Express read local env vars rather than the bundle's resource bindings
when running outside a deployed App.

## 3. Non-secret identifiers already committed (for reference)

These are **not credentials** and are already in the repo — listed here so
it's clear what is and isn't sensitive:

- Workspace host: `adb-7405617014325769.9.azuredatabricks.net` (`databricks.yml`)
- SQL warehouse ID, three job IDs, UC volume path (`apps/migration-dashboard/migration-dashboard/databricks.yml`)
- The generated-code owner's Databricks user UUID (`GENERATED_CODE_OWNER` env var / hardcoded fallback in `server/custom-routes.ts`) — an opaque internal identifier, not a credential

## 4. Quick checklist for a fresh machine

1. `databricks auth login ...` or hand-write the `[dev]` profile (step 1).
2. `python3 setup/bootstrap_workspace.py --profile dev` — idempotent, safe
   to re-run; creates the catalog/schemas/volume/secret-scope/tables if they
   don't already exist in this workspace, or confirms they do.
3. `cd apps/migration-dashboard/migration-dashboard && cp` the `.env` shape
   above (step 2) if running the dashboard locally; not needed for a
   `databricks apps deploy` (that path reads `app.yaml`'s bindings instead).
4. `databricks apps deploy --profile dev` from inside the dashboard
   directory to deploy/update the running app.
