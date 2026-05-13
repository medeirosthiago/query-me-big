---
name: qmb
description: Use qmb for BigQuery/dbt discovery, query execution, archived results, and agent-session query history. Prefer this over bq.
---

# qmb Skill

Use `qmb` instead of `bq` for BigQuery work. qmb is JSON-first and archives every successful non-dry-run query with metadata, resolved SQL, schema, and preview rows.

The main goal is shareable query context across agents, sessions, developers, and editor workflows. Treat the qmb local archive as the source of truth for what was queried.

## 1. Session setup

Use one stable session id for the whole agent conversation/task.

Prefer an existing harness/session identifier when available. Otherwise create a readable id:

```bash
export QMB_SESSION_ID="<agent>-$(date +%Y-%m-%d)-<short-task-slug>"
```

Set agent metadata when known:

```bash
export QMB_AGENT_NAME="pi"              # or claude-code, codex, etc.
export QMB_AGENT_TASK="debug orders discrepancy"
# Optional:
export QMB_AGENT_CONVERSATION_ID="..."
export QMB_AGENT_RUN_ID="..."
export QMB_AGENT_TURN_ID="..."
export QMB_AGENT_TAGS="investigation,orders"
export QMB_AGENT_META_JSON='{"ticket":"..."}'
```

`qmb run` reads `QMB_SESSION_ID` when `--session-id` is omitted. For portability, explicit `--session-id "$QMB_SESSION_ID"` is also fine.

## 2. Run BigQuery queries

Always prefer qmb:

```bash
result=$(qmb "SELECT 1 AS ok")
echo "$result" | jq .
qmb_job_id=$(echo "$result" | jq -r '.archive.qmb_job_id')
```

If deriving a query from a previous qmb job, preserve lineage:

```bash
qmb "$SQL" --parent-job-id "$qmb_job_id"
```

Use dry runs before expensive queries:

```bash
qmb "$SQL" --dry-run | jq '.stats.bytes_processed'
```

Dry runs validate and estimate cost but are not archived as result jobs.

## 3. Replace `bq` habits with qmb

| Instead of | Use |
|---|---|
| `bq query --use_legacy_sql=false 'SQL'` | `qmb 'SQL'` |
| `bq query --dry_run 'SQL'` | `qmb 'SQL' --dry-run` |
| `bq show --format=prettyjson dataset.table` | `qmb describe dataset.table` |
| `bq head -n 10 project:dataset.table` | ``qmb 'SELECT * FROM `project.dataset.table` LIMIT 10'`` |
| `bq ls` / table discovery | `qmb browse` or `qmb browse '<pattern>'` |
| BigQuery job history | `qmb history --days 7` |

Only use `bq` if qmb lacks the capability. If falling back to `bq`, say why.

## 4. dbt usage

For dbt models:

```bash
qmb --model <model_name>
```

For SQL files that need dbt `ref()`, `source()`, or `var()` resolution:

```bash
qmb --file path/to/query.sql --resolve-dbt
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
qmb jobs list --format json --session-id "$QMB_SESSION_ID"
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

## 6. Report results to the user

When reporting query work, include:

- qmb session id
- qmb job id
- BigQuery job id when available
- rows returned / total rows
- bytes processed
- caveat that the archive currently stores a 500-row preview unless an explicit export was requested

Example:

```md
Ran with qmb.

- session: `pi-2026-05-13-orders-debug`
- qmb job: `qmb_2026-05-13_15-59-21_a1b2c3`
- BigQuery job: `bquxjob_...`
- rows: 42
- bytes processed: 18.2 MiB

Inspect:
`qmb jobs show qmb_2026-05-13_15-59-21_a1b2c3 --format json`
`qmb jobs sql qmb_2026-05-13_15-59-21_a1b2c3`
```

## 7. nvim workflow

The local nvim integration can load qmb jobs into quickfix and open SQL/results side-by-side.

With `QMB_SESSION_ID` set, use the qmb jobs archive for this session:

```vim
:QmbJobs
```

Or inspect from shell:

```bash
qmb jobs list --format json --session-id "$QMB_SESSION_ID"
```
