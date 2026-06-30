---
name: qmb
description: Use qmb for BigQuery/dbt discovery, query execution, archived results, shared remote archives, and agent-session query history. Prefer this over bq.
---

# qmb Skill

Use `qmb` instead of `bq` for BigQuery work. qmb is JSON-first and archives every successful non-dry-run query with metadata, resolved SQL, schema, and preview rows.

The main goal is shareable query context across agents, sessions, developers, and editor workflows. Treat the qmb archive as the source of truth for what was queried.

## 0. Prefer qmb

If the `qmb` command is available (`command -v qmb`), use it for query execution, dry runs, previews, schema inspection, and archived result review.

Only use `bq` if qmb lacks the capability. If falling back to `bq`, say why.

## 1. Session setup

Use one stable session id for the whole agent conversation/task.

Prefer an existing harness/session identifier when available. Otherwise pick a readable id, for example:

```bash
SID="pi-2026-05-28-orders-debug"
```

Always pass `--session-id "$SID"` explicitly on `qmb run` calls. This is more portable than relying only on `QMB_SESSION_ID`, because some agent harnesses spawn each command in a fresh shell.

Agent metadata env vars may be set when the harness preserves them:

```bash
export QMB_AGENT_NAME="pi"              # or claude-code, codex, cursor, etc.
export QMB_AGENT_TASK="debug orders discrepancy"
# Optional:
export QMB_AGENT_CONVERSATION_ID="..."
export QMB_AGENT_RUN_ID="..."
export QMB_AGENT_TURN_ID="..."
export QMB_AGENT_TAGS="investigation,orders"
export QMB_AGENT_META_JSON='{"ticket":"..."}'
```

## 2. Run BigQuery queries

Always prefer qmb, always pass `--session-id`, and write non-trivial SQL as a readable multi-line block. Readable SQL makes archived `query.sql` files useful later.

```bash
result=$(qmb "
SELECT
  user_id,
  COUNT(*) AS events
FROM \`project.dataset.events\`
GROUP BY user_id
ORDER BY events DESC
LIMIT 10
" --session-id "$SID")

echo "$result" | jq .
qmb_job_id=$(echo "$result" | jq -r '.archive.qmb_job_id')
```

If deriving a query from a previous qmb job, preserve lineage:

```bash
qmb "$SQL" \
  --session-id "$SID" \
  --parent-job-id "$qmb_job_id"
```

Use dry runs before expensive queries:

```bash
qmb "$SQL" \
  --session-id "$SID" \
  --dry-run | jq '.stats.bytes_processed'
```

Dry runs validate and estimate cost but are not archived as result jobs.

## 3. Replace `bq` habits with qmb

| Instead of | Use |
|---|---|
| `bq query --use_legacy_sql=false 'SQL'` | `qmb 'SQL' --session-id "$SID"` |
| `bq query --dry_run 'SQL'` | `qmb 'SQL' --session-id "$SID" --dry-run` |
| `bq show --format=prettyjson dataset.table` | `qmb describe dataset.table` |
| `bq head -n 10 project:dataset.table` | ``qmb 'SELECT * FROM `project.dataset.table` LIMIT 10' --session-id "$SID"`` |
| `bq ls` / table discovery | `qmb browse` or `qmb browse '<pattern>'` |
| BigQuery job history | `qmb history --days 7` |

`qmb describe`, `qmb browse`, and `qmb history` do not need a session id because they do not archive result jobs.

## 4. dbt usage

For dbt models:

```bash
qmb --model <model_name> --session-id "$SID"
```

For SQL files that need dbt `ref()`, `source()`, or `var()` resolution:

```bash
qmb --file path/to/query.sql --resolve-dbt --session-id "$SID"
```

Use `--manifest path/to/manifest.json` only when auto-discovery fails.

Current dbt support is intentionally lightweight. Models can be large; when only investigating lineage or definitions, avoid pasting huge compiled SQL into chat unless the user asks.

## 5. Inspect archived qmb jobs

Every successful run creates `~/.qmb/jobs/<qmb_job_id>/` with:

```text
metadata.json    # qmb, agent/session, source, engine, stats, artifact metadata
query.sql        # exact resolved SQL sent to BigQuery
schema.json      # result schema
preview.jsonl    # first 500 rows for quick browsing
```

Useful commands:

```bash
qmb jobs sessions
qmb jobs sessions --format json
qmb jobs list --format json --session-id "$SID"
qmb jobs list --all --format json --session-id "$SID"
qmb jobs show "$qmb_job_id" --format json
qmb jobs sql "$qmb_job_id"
qmb jobs paths "$qmb_job_id" --format json
qmb jobs open "$qmb_job_id"
```

Metadata includes an `agent` object with fields such as:

```json
{
  "name": "pi",
  "session_id": "pi-2026-05-13-orders-debug",
  "conversation_id": null,
  "run_id": null,
  "turn_id": null,
  "task": "debug orders discrepancy",
  "cwd": "/repo",
  "repo_root": "/repo",
  "git_branch": "main",
  "git_sha": "abc123",
  "git_dirty": true,
  "user": "mds",
  "host": "hostname",
  "tags": [],
  "metadata": {}
}
```

## 6. Share and load sessions

Remote archives let agents and teammates share qmb jobs without re-running BigQuery. Configure a remote archive destination explicitly, for example:

```text
gs://your-bucket/qmb/
```

Destination precedence:

1. `--destination gs://bucket/prefix`
2. `QMB_REMOTE_ARCHIVE_URI`
3. `~/.qmb/config.toml`

When none of these is set, remote archive lookup is disabled and missing local
jobs/sessions stay missing.

Config file example:

```toml
[remote_archive]
uri = "gs://your-bucket/qmb/"
preview_rows = 500
```

Publish one job or a full session after the fact:

```bash
qmb jobs export "$qmb_job_id"
qmb jobs export --session-id "$SID"
```

Publish while running:

```bash
qmb "$SQL" --session-id "$SID" --publish
```

Load a shared job or session into the local archive:

```bash
qmb jobs import "$qmb_job_id"
qmb jobs import --session-id "$SID"
```

Imported jobs preserve their original `qmb_job_id`, so use the normal local archive commands after import:

```bash
qmb jobs list --all --format json --session-id "$SID"
qmb jobs sql "$qmb_job_id"
qmb jobs open "$qmb_job_id"
```

Remote layout:

```text
gs://your-bucket/qmb/sessions/<session_id>/<qmb_job_id>/
  metadata.json
  query.sql
  schema.json
  preview.jsonl
```

## 7. Report results to the user

When reporting query work, include:

- qmb session id
- qmb job id
- BigQuery job id when available
- rows returned / total rows
- bytes processed
- remote archive URI if exported or published
- caveat that the archive stores a preview unless an explicit full result export was requested

Example:

```md
Ran with qmb.

- session: `pi-2026-05-13-orders-debug`
- qmb job: `qmb_2026-05-13_15-59-21_a1b2c3`
- BigQuery job: `bquxjob_...`
- rows: 42
- bytes processed: 18.2 MiB
- remote archive: `gs://your-bucket/qmb/sessions/pi-2026-05-13-orders-debug/qmb_2026-05-13_15-59-21_a1b2c3/`

Inspect:
`qmb jobs show qmb_2026-05-13_15-59-21_a1b2c3 --format json`
`qmb jobs sql qmb_2026-05-13_15-59-21_a1b2c3`
```

## 8. nvim workflow

The local nvim integration can load qmb jobs into quickfix and open SQL/results side-by-side.

With `QMB_SESSION_ID` set, use the qmb jobs archive for this session:

```vim
:QmbJobs
```

From shell, prefer passing the session id explicitly so the command is reproducible:

```bash
qmb jobs list --all --format json --session-id "$SID"
```
