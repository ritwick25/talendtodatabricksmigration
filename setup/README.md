# Workspace setup

`bootstrap_workspace.py` provisions everything this system needs in a
Databricks workspace: catalog, `migration`/`samples` schemas, the
`talend_export` volume (with `samples/`, `real/`, `jars/`, `jars_src/`
subdirectories), a secret scope, all control-plane Delta tables, and the
seed `control` row. It is idempotent — safe to re-run, and every step
checks for existence before creating anything.

## Moving this repo to a different workspace

1. Point the CLI at the new workspace — either:
   ```bash
   databricks auth login --host https://<new-workspace>.azuredatabricks.net --profile dev
   ```
   or write a `[dev]` profile by hand into `~/.databrickscfg` (OAuth M2M
   service principal: `host`, `client_id`, `client_secret`).
2. Run:
   ```bash
   python3 setup/bootstrap_workspace.py --profile dev
   ```
3. Grant yourself (or whoever needs to see this in Catalog Explorer)
   access — the script deliberately does **not** grant access to anyone
   by default, since that's a privilege-escalation action. Either pass
   `--grant-to you@example.com`, or run the printed `GRANT` statement
   yourself in the SQL editor.

## Overriding names

All names are flags, defaulting to this project's current values:

```bash
python3 setup/bootstrap_workspace.py \
  --profile dev \
  --catalog talend_migration \
  --control-schema migration \
  --samples-schema samples \
  --volume talend_export \
  --secret-scope talend-migration \
  --grant-to you@example.com
```

Requires only the `databricks` CLI on `PATH` and Python 3 stdlib — no
extra dependencies.
