"""CLI entrypoint for qmb."""

from __future__ import annotations

import json
import re
from dataclasses import replace
from datetime import UTC, date, datetime, time
from pathlib import Path
from typing import TYPE_CHECKING, Annotated, Any

import click
import typer
from rich.console import Console
from typer.core import TyperGroup

if TYPE_CHECKING:
    from qmb.application.outcomes import ExecutionOutcome
    from qmb.bigquery.history import QueryHistoryEntry
    from qmb.formatters import Format
    from qmb.types import QueryRequest


class _DefaultRunGroup(TyperGroup):
    """Typer group that falls back to the 'run' command for unknown args.

    Also installs a structured JSON error handler: every exception that
    escapes a command body is converted into a one-line JSON object on
    stderr with a categorized exit code (see :mod:`qmb.errors`).
    """

    def parse_args(self, ctx: click.Context, args: list[str]) -> list[str]:
        # Let top-level help / version flags reach the group itself so users
        # can discover subcommands and check the installed version without
        # being routed into the `run` command.
        if (
            args
            and args[0] not in self.commands
            and args[0] not in {"--help", "-h", "--version", "-V"}
        ):
            args = ["run", *args]
        return super().parse_args(ctx, args)

    def main(self, *args: Any, **kwargs: Any) -> Any:
        # We always run the JSON error handler so the agent-facing
        # contract is identical across real invocations and tests.
        # ``emit_json_error`` calls ``sys.exit`` so test runners observe
        # the categorized exit code via the resulting ``SystemExit``.
        kwargs.pop("standalone_mode", None)
        try:
            result = super().main(*args, standalone_mode=False, **kwargs)
            # Typer converts KeyboardInterrupt into click.exceptions.Exit(130)
            # which, with standalone_mode=False, becomes the return value
            # rather than a raised exception. Surface it explicitly.
            if isinstance(result, int) and result != 0:
                from qmb.errors import (
                    EXIT_ENGINE_ERROR,
                    EXIT_INTERRUPTED,
                    emit_json_error,
                )

                if result == EXIT_INTERRUPTED:
                    emit_json_error(
                        type_="interrupted",
                        message="Aborted by user",
                        exit_code=EXIT_INTERRUPTED,
                    )
                emit_json_error(
                    type_="internal_error",
                    message=f"Process exited with code {result}",
                    exit_code=result or EXIT_ENGINE_ERROR,
                )
            return result
        except click.exceptions.NoArgsIsHelpError:
            # ``no_args_is_help=True`` intentionally prints help for bare
            # groups (``qmb`` / ``qmb jobs``). With ``standalone_mode=False``
            # Click raises after printing; treat that path as a clean help
            # display rather than emitting the structured error contract.
            return None
        except click.exceptions.UsageError as e:
            # BadParameter, MissingParameter, NoSuchOption, generic UsageError.
            from qmb.errors import EXIT_USER_ERROR, emit_json_error

            emit_json_error(
                type_="user_error",
                message=e.format_message(),
                exit_code=EXIT_USER_ERROR,
                details={"class": type(e).__name__},
            )
        except click.exceptions.ClickException as e:
            from qmb.errors import EXIT_USER_ERROR, emit_json_error

            emit_json_error(
                type_="user_error",
                message=e.format_message(),
                exit_code=EXIT_USER_ERROR,
                details={"class": type(e).__name__},
            )
        except (KeyboardInterrupt, click.exceptions.Abort):
            from qmb.errors import EXIT_INTERRUPTED, emit_json_error

            emit_json_error(
                type_="interrupted",
                message="Aborted by user",
                exit_code=EXIT_INTERRUPTED,
            )
        except (FileNotFoundError, PermissionError, IsADirectoryError) as e:
            from qmb.errors import EXIT_USER_ERROR, emit_json_error

            emit_json_error(
                type_="user_error",
                message=str(e),
                exit_code=EXIT_USER_ERROR,
                details={"class": type(e).__name__},
            )
        except Exception as e:
            if type(e).__name__ == "NoArgsIsHelpError":
                return None
            if hasattr(e, "format_message"):
                from qmb.errors import EXIT_USER_ERROR, emit_json_error

                emit_json_error(
                    type_="user_error",
                    message=e.format_message(),
                    exit_code=EXIT_USER_ERROR,
                    details={"class": type(e).__name__},
                )

            from qmb.errors import EXIT_ENGINE_ERROR, emit_json_error

            error_type = _classify_exception(e)
            emit_json_error(
                type_=error_type,
                message=str(e) or type(e).__name__,
                exit_code=EXIT_ENGINE_ERROR,
                details={"class": type(e).__name__},
            )


def _classify_exception(exc: BaseException) -> str:
    """Categorize an unhandled exception for the JSON ``error.type`` field."""
    try:
        from google.api_core.exceptions import GoogleAPIError
    except ImportError:
        GoogleAPIError = ()  # type: ignore[assignment,misc]
    if GoogleAPIError and isinstance(exc, GoogleAPIError):
        return "engine_error"
    return "internal_error"


app = typer.Typer(
    name="qmb",
    help="Query Me Big – Run BigQuery queries with a Textual TUI, dbt support, and export.",
    no_args_is_help=True,
    cls=_DefaultRunGroup,
)
jobs_app = typer.Typer(help="Inspect local qmb job archives.", no_args_is_help=True)
app.add_typer(jobs_app, name="jobs")
console = Console()


def _version_callback(value: bool) -> None:
    if value:
        from qmb import __version__

        typer.echo(f"qmb {__version__}")
        raise typer.Exit()


@app.callback()
def _main(
    version: Annotated[
        bool,
        typer.Option(
            "--version",
            "-V",
            callback=_version_callback,
            is_eager=True,
            help="Show qmb version and exit",
        ),
    ] = False,
) -> None:
    """Query Me Big – BigQuery CLI with Textual TUI, dbt support, and export."""
    # Body intentionally empty — the callback exists so Typer attaches
    # --version to the top-level group.
    return None

_INT_PATTERN = re.compile(r"[+-]?(?:0|[1-9]\d*)\Z")
_FLOAT_PATTERN = re.compile(
    r"[+-]?(?:\d+\.\d*|\d*\.\d+|\d+[eE][+-]?\d+|\d+\.\d*[eE][+-]?\d+|\d*\.\d+[eE][+-]?\d+)\Z"
)
_DATE_ONLY_PATTERN = re.compile(r"\d{4}-\d{2}-\d{2}\Z")


def _coerce_var_value(raw_value: str) -> Any:
    """Parse a CLI var into a conservative Python scalar."""
    lowered = raw_value.casefold()
    if lowered == "true":
        return True
    if lowered == "false":
        return False
    if lowered in {"null", "none"}:
        return None
    if _INT_PATTERN.fullmatch(raw_value):
        return int(raw_value)
    if _FLOAT_PATTERN.fullmatch(raw_value):
        return float(raw_value)
    return raw_value


def _parse_key_value_pairs(items: list[str] | None, *, option_name: str) -> dict[str, Any]:
    """Parse repeatable key=value CLI options into a mapping."""
    if not items:
        return {}
    values: dict[str, Any] = {}
    for item in items:
        if "=" not in item:
            raise typer.BadParameter(f"Invalid {option_name} format: '{item}'. Use key=value.")
        key, _, value = item.partition("=")
        key = key.strip()
        if not key:
            raise typer.BadParameter(f"Invalid {option_name} format: '{item}'. Key is empty.")
        values[key] = _coerce_var_value(value.strip())
    return values


def _parse_vars(var_list: list[str] | None) -> dict[str, Any]:
    """Parse --var key=value pairs."""
    return _parse_key_value_pairs(var_list, option_name="--var")


def _parse_agent_metadata(meta_list: list[str] | None) -> dict[str, Any]:
    """Parse --meta key=value pairs for agent metadata."""
    return _parse_key_value_pairs(meta_list, option_name="--meta")


@app.command()
def run(
    query: Annotated[
        str | None,
        typer.Argument(help="Inline SQL query to execute"),
    ] = None,
    file: Annotated[
        Path | None,
        typer.Option("--file", "-f", help="Path to a .sql file to execute"),
    ] = None,
    model: Annotated[
        str | None,
        typer.Option("--model", "-m", help="dbt model name (uses compiled SQL from manifest)"),
    ] = None,
    manifest: Annotated[
        Path | None,
        typer.Option("--manifest", help="Path to dbt manifest.json"),
    ] = None,
    resolve_dbt: Annotated[
        bool,
        typer.Option("--resolve-dbt/--no-resolve-dbt", help="Resolve ref/source/var in SQL files"),
    ] = False,
    var: Annotated[
        list[str] | None,
        typer.Option("--var", "-v", help="dbt variable override: key=value (repeatable)"),
    ] = None,
    project: Annotated[
        str | None,
        typer.Option("--project", help="GCP project ID"),
    ] = None,
    location: Annotated[
        str | None,
        typer.Option("--location", help="BigQuery location (e.g. US, EU)"),
    ] = None,
    page_size: Annotated[
        int,
        typer.Option("--page-size", help="Rows per page in TUI"),
    ] = 200,
    export: Annotated[
        str | None,
        typer.Option("--export", "-e", help="Export format: csv, json, or parquet"),
    ] = None,
    out: Annotated[
        Path | None,
        typer.Option("--out", "-o", help="Export output path"),
    ] = None,
    publish: Annotated[
        bool,
        typer.Option(
            "--publish",
            help=(
                "Publish the local qmb job archive to remote storage after a successful run. "
                "Requires a configured destination (--destination, QMB_REMOTE_ARCHIVE_URI, "
                "or [remote_archive].uri in ~/.qmb/config.toml)."
            ),
        ),
    ] = False,
    destination: Annotated[
        str | None,
        typer.Option(
            "--destination",
            help=(
                "Remote archive destination URI for --publish. Defaults to "
                "QMB_REMOTE_ARCHIVE_URI or ~/.qmb/config.toml; required when --publish "
                "is set and neither is configured."
            ),
        ),
    ] = None,
    tui: Annotated[
        bool,
        typer.Option(
            "--tui",
            "-t",
            help="Open the interactive Textual TUI instead of printing JSON",
        ),
    ] = False,
    session_id: Annotated[
        str | None,
        typer.Option(
            "--session-id",
            help=(
                "Tag this run with an agent/session identifier. Persisted "
                "in the local archive and surfaced in the JSON output so "
                "`qmb jobs list --session-id <id>` can recover the group. "
                "Defaults to QMB_SESSION_ID when unset."
            ),
        ),
    ] = None,
    parent_job_id: Annotated[
        str | None,
        typer.Option(
            "--parent-job-id",
            help=(
                "Reference a prior qmb job id this run derives from. "
                "Persisted in the archive for later tree/graph navigation."
            ),
        ),
    ] = None,
    agent: Annotated[
        str | None,
        typer.Option(
            "--agent",
            help="Agent/tool name for archive metadata (defaults to QMB_AGENT_NAME).",
        ),
    ] = None,
    agent_conversation_id: Annotated[
        str | None,
        typer.Option(
            "--agent-conversation-id",
            help="Conversation identifier for archive metadata.",
        ),
    ] = None,
    agent_run_id: Annotated[
        str | None,
        typer.Option(
            "--agent-run-id",
            help="Agent run identifier for archive metadata.",
        ),
    ] = None,
    agent_turn_id: Annotated[
        str | None,
        typer.Option(
            "--agent-turn-id",
            help="Agent turn identifier for archive metadata.",
        ),
    ] = None,
    agent_task: Annotated[
        str | None,
        typer.Option(
            "--agent-task",
            help="Human-readable task label for archive metadata.",
        ),
    ] = None,
    tag: Annotated[
        list[str] | None,
        typer.Option(
            "--tag",
            help="Tag for the archived run (repeatable; env QMB_AGENT_TAGS is comma-separated).",
        ),
    ] = None,
    meta: Annotated[
        list[str] | None,
        typer.Option(
            "--meta",
            help="Agent metadata key=value pair (repeatable; merged with QMB_AGENT_META_JSON).",
        ),
    ] = None,
    dry_run: Annotated[
        bool,
        typer.Option("--dry-run", help="Validate query without executing"),
    ] = False,
    max_bytes_billed: Annotated[
        int | None,
        typer.Option("--max-bytes-billed", help="Maximum bytes billed safety limit"),
    ] = None,
    where: Annotated[
        str | None,
        typer.Option("--where", "-w", help="WHERE clause appended to the resolved SQL"),
    ] = None,
    output_format: Annotated[
        str,
        typer.Option(
            "--format",
            help="Output format: json (default), csv, table, tui.",
        ),
    ] = "json",
) -> None:
    """Run a BigQuery query with optional dbt model resolution.

    Prints a JSON object to stdout by default so the output can be piped
    into ``jq`` or consumed by agents. Pass ``-t`` / ``--tui`` to open
    the interactive Textual app instead.
    """
    from qmb.formatters import Format
    from qmb.types import ExportFormat, InputMode, QueryRequest

    # ``-t`` / ``--tui`` is a convenience alias for ``--format tui``. If
    # both are given, the explicit ``--format`` wins.
    try:
        selected_format = Format.parse(output_format)
    except ValueError as e:
        raise typer.BadParameter(str(e)) from e
    if tui and output_format == "json":
        selected_format = Format.TUI
    # ``no_tui`` is a leftover field on QueryRequest used by the TUI
    # formatter to decide whether to launch; ``True`` means “don’t
    # launch”, which is everything except ``--format tui``.
    no_tui = selected_format is not Format.TUI

    # Validate mutually exclusive inputs
    inputs = sum(x is not None for x in [query, file, model])
    if inputs == 0:
        raise typer.BadParameter("Provide one of query, --file, or --model.")
    if inputs > 1:
        raise typer.BadParameter("Provide only one of query, --file, or --model.")

    # Determine mode
    if query is not None:
        mode = InputMode.SQL
    elif file is not None:
        if str(file) == "-":
            import sys

            mode = InputMode.SQL
            query = sys.stdin.read()
            file = None
            if not query.strip():
                raise typer.BadParameter("No SQL provided on stdin.")
        else:
            mode = InputMode.FILE
            if not file.exists():
                raise typer.BadParameter(f"File not found: {file}")
    else:
        mode = InputMode.MODEL

    # Auto-enable dbt resolution when file is inside a dbt project or env vars are set
    if mode == InputMode.FILE and not resolve_dbt:
        from qmb.dbt.manifest import has_dbt_env, is_dbt_project_file

        if is_dbt_project_file(file) or has_dbt_env():  # type: ignore[arg-type]
            resolve_dbt = True
            console.print("[dim]Auto-detected dbt project, enabling --resolve-dbt[/dim]")

    needs_manifest = mode == InputMode.MODEL or (mode == InputMode.FILE and resolve_dbt)
    if needs_manifest and not manifest:
        from qmb.dbt.manifest import discover_manifest_path

        try:
            manifest = discover_manifest_path()
            console.print(f"[dim]Using manifest: {manifest}[/dim]")
        except FileNotFoundError as e:
            raise typer.BadParameter(str(e)) from e

    # Parse export format
    export_format = None
    if export:
        try:
            export_format = ExportFormat(export.lower())
        except ValueError as e:
            raise typer.BadParameter(
                f"Invalid export format: {export}. Use csv, json, or parquet."
            ) from e

    if export_format and not out:
        ext = {"csv": ".csv", "json": ".json", "parquet": ".parquet"}[export_format.value]
        out = Path(f"output{ext}")

    from qmb.agent import build_agent_context, effective_session_id

    variables = _parse_vars(var)
    agent_metadata = _parse_agent_metadata(meta)
    session_id = effective_session_id(session_id)
    agent_context = build_agent_context(
        session_id=session_id,
        name=agent,
        conversation_id=agent_conversation_id,
        run_id=agent_run_id,
        turn_id=agent_turn_id,
        task=agent_task,
        tags=tag,
        metadata=agent_metadata,
    )

    request = QueryRequest(
        mode=mode,
        sql=query,
        file_path=file,
        model_name=model,
        manifest_path=manifest,
        resolve_dbt=resolve_dbt,
        variables=variables,
        project=project,
        location=location,
        page_size=page_size,
        export_format=export_format,
        export_path=out,
        no_tui=no_tui,
        dry_run=dry_run,
        max_bytes_billed=max_bytes_billed,
        where=where,
        session_id=session_id,
        parent_job_id=parent_job_id,
        agent_context=agent_context,
    )

    if publish and not dry_run:
        from qmb.config import remote_archive_uri

        if remote_archive_uri(destination) is None:
            raise typer.BadParameter(
                "Remote archive destination is not configured, but --publish was set. "
                "Set --destination, QMB_REMOTE_ARCHIVE_URI, or [remote_archive].uri "
                "in ~/.qmb/config.toml before running the query."
            )

    _execute(request, output_format=selected_format, publish=publish, destination=destination)




@app.command()
def history(
    days: Annotated[
        int,
        typer.Option("--days", "-d", help="Number of days to look back"),
    ] = 7,
    project: Annotated[
        str | None,
        typer.Option("--project", help="GCP project ID"),
    ] = None,
    location: Annotated[
        str | None,
        typer.Option("--location", help="BigQuery location (e.g. US, EU)"),
    ] = None,
    limit: Annotated[
        int,
        typer.Option("--limit", "-l", help="Maximum number of queries to fetch"),
    ] = 200,
    user_email: Annotated[
        str | None,
        typer.Option(
            "--user",
            "--email",
            help=(
                "Filter by user email (e.g. you@example.com). Fetches "
                "all users' jobs and filters client-side; requires "
                "bigquery.jobs.listAll IAM permission."
            ),
        ),
    ] = None,
    page_size: Annotated[
        int,
        typer.Option("--page-size", help="Rows per page in TUI"),
    ] = 200,
    tui: Annotated[
        bool,
        typer.Option(
            "--tui",
            "-t",
            help="Open the interactive history picker instead of printing JSON",
        ),
    ] = False,
) -> None:
    """Browse recent BigQuery query history.

    Prints recent BigQuery jobs as a JSON array on stdout by default.
    Pass ``-t`` / ``--tui`` to open the interactive picker instead.
    By default only the current user's jobs are listed; pass ``--user`` to
    filter by another user's email (requires ``bigquery.jobs.listAll``).
    """
    from qmb.bigquery.client import get_client
    from qmb.bigquery.history import list_recent_queries

    client = get_client(project, location)
    entries = list_recent_queries(client, days=days, limit=limit, user_email=user_email)

    if not tui:
        payload = [_history_entry_to_dict(entry) for entry in entries]
        typer.echo(json.dumps(payload, default=str))
        return

    from qmb.tui.app import QueryResultApp
    from qmb.types import QueryResultHandle

    if not entries:
        console.print("[yellow]No recent queries found.[/yellow]")
        return

    console.print(f"[green]✓[/green] Found {len(entries)} recent queries")

    app_instance = QueryResultApp(
        bq_client=client,
        handle=QueryResultHandle(
            job_id="",
            project=client.project or project or "",
            location=location or getattr(client, "location", None) or "",
            destination_table="",
            schema=[],
            total_rows=0,
        ),
        source_label="history",
        page_size=page_size,
        start_in_browser=False,
        browser_only=False,
        history_entries=entries,
    )
    app_instance.run()


def _history_entry_to_dict(entry: QueryHistoryEntry) -> dict[str, Any]:
    return {
        "job_id": entry.job_id,
        "project": entry.project,
        "location": entry.location,
        "created": entry.created.isoformat() if entry.created else None,
        "query": entry.query,
        "bytes_processed": entry.bytes_processed,
        "state": entry.state,
        "user_email": entry.user_email,
    }


@jobs_app.command("list")
def jobs_list(
    output_format: Annotated[
        str,
        typer.Option("--format", help="Output format: text or json"),
    ] = "text",
    session_id: Annotated[
        str | None,
        typer.Option(
            "--session-id",
            "--session",
            help="Only show jobs tagged with this session id (set on `qmb run`).",
        ),
    ] = None,
    parent_job_id: Annotated[
        str | None,
        typer.Option(
            "--parent-job-id",
            help="Only show jobs that descend from this parent qmb job id.",
        ),
    ] = None,
    limit: Annotated[
        int | None,
        typer.Option(
            "--limit",
            "-l",
            help="Number of records returned, newest first (default: 50; use --all for all).",
        ),
    ] = None,
    show_all: Annotated[
        bool,
        typer.Option(
            "--all",
            help="Return all matching jobs instead of the default newest 50.",
        ),
    ] = False,
    agent_name: Annotated[
        str | None,
        typer.Option("--agent", help="Only show jobs whose agent/tool name contains this text."),
    ] = None,
    date_filter: Annotated[
        str | None,
        typer.Option("--date", help="Only show jobs created on this UTC date (YYYY-MM-DD)."),
    ] = None,
    since: Annotated[
        str | None,
        typer.Option("--since", help="Only show jobs created at/after this UTC date or ISO time."),
    ] = None,
    until: Annotated[
        str | None,
        typer.Option("--until", help="Only show jobs created at/before this UTC date or ISO time."),
    ] = None,
    file_filter: Annotated[
        str | None,
        typer.Option(
            "--file",
            help="Only show jobs whose archived file path/label contains this text.",
        ),
    ] = None,
    model_filter: Annotated[
        str | None,
        typer.Option("--model", help="Only show jobs whose model name/label contains this text."),
    ] = None,
    source_filter: Annotated[
        str | None,
        typer.Option(
            "--source",
            help="Only show jobs whose source label/path/model/node contains this text.",
        ),
    ] = None,
    query_filter: Annotated[
        str | None,
        typer.Option("--query", help="Only show jobs whose archived SQL contains this text."),
    ] = None,
    destination: Annotated[
        str | None,
        typer.Option(
            "--destination",
            help=(
                "Remote archive URI used to auto-import --session-id when "
                "the session is missing locally."
            ),
        ),
    ] = None,
) -> None:
    """List qmb job archives.

    When --session-id has no local matches, tries to import the session from
    the configured remote archive before listing.
    """
    from qmb.jobs.store import JobStore

    if output_format not in {"text", "json"}:
        raise typer.BadParameter("Invalid format. Use text or json.")
    if show_all and limit is not None:
        raise typer.BadParameter("Use either --all or --limit, not both.")
    if limit is not None and limit < 0:
        raise typer.BadParameter("--limit must be zero or greater.")

    effective_limit = None if show_all else (limit if limit is not None else 50)

    store = JobStore()
    all_records = store.list()
    session_missing = session_id is not None and not any(
        _record_session_id(record) == session_id for record in all_records
    )
    if session_missing:
        _try_import_remote_session(session_id, store, destination=destination)
        all_records = store.list()
    records = _filter_job_records(
        all_records,
        session_id=session_id,
        parent_job_id=parent_job_id,
        agent_name=agent_name,
        date_filter=date_filter,
        since=since,
        until=until,
        file_filter=file_filter,
        model_filter=model_filter,
        source_filter=source_filter,
        query_filter=query_filter,
    )
    if effective_limit is not None:
        records = records[:effective_limit]

    if output_format == "json":
        payload = [_job_record_to_list_metadata(record) for record in records]
        typer.echo(json.dumps(payload, indent=2))
        return

    if not records:
        typer.echo("No local qmb jobs found.")
        return

    for record in records:
        session = _record_session_id(record) or "-"
        typer.echo(
            f"{record.created_at:%Y-%m-%d %H:%M:%S}  "
            f"{record.qmb_job_id}  session:{session}  "
            f"{record.source.label}  {record.total_rows:,} rows"
        )


def _filter_job_records(
    records: list[Any],
    *,
    session_id: str | None,
    parent_job_id: str | None,
    agent_name: str | None,
    date_filter: str | None,
    since: str | None,
    until: str | None,
    file_filter: str | None,
    model_filter: str | None,
    source_filter: str | None,
    query_filter: str | None,
) -> list[Any]:
    """Apply ``qmb jobs list`` filters, preserving the newest-first order."""
    filtered = records
    created_date = _parse_job_date_filter(date_filter)
    since_dt = _parse_job_datetime_filter(since, end_of_day=False)
    until_dt = _parse_job_datetime_filter(until, end_of_day=True)

    if session_id is not None:
        filtered = [record for record in filtered if _record_session_id(record) == session_id]
    if parent_job_id is not None:
        filtered = [record for record in filtered if record.parent_job_id == parent_job_id]
    if agent_name is not None:
        filtered = [
            record for record in filtered if _any_contains(agent_name, _record_agent_name(record))
        ]
    if created_date is not None:
        filtered = [
            record for record in filtered if _record_created_at_utc(record).date() == created_date
        ]
    if since_dt is not None:
        filtered = [record for record in filtered if _record_created_at_utc(record) >= since_dt]
    if until_dt is not None:
        filtered = [record for record in filtered if _record_created_at_utc(record) <= until_dt]
    if file_filter is not None:
        filtered = [
            record
            for record in filtered
            if _any_contains(file_filter, record.source.file_path, record.source.label)
        ]
    if model_filter is not None:
        filtered = [
            record
            for record in filtered
            if _any_contains(model_filter, record.source.model_name, record.source.label)
        ]
    if source_filter is not None:
        filtered = [
            record
            for record in filtered
            if _any_contains(
                source_filter,
                record.source.label,
                record.source.file_path,
                record.source.model_name,
                record.source.matched_node_id,
            )
        ]
    if query_filter is not None:
        filtered = [
            record for record in filtered if _any_contains(query_filter, _read_job_sql(record))
        ]
    return filtered


def _job_record_to_list_metadata(record: Any) -> dict[str, Any]:
    """Return JSON for ``jobs list`` with a session fallback for old archives."""
    payload = record.to_metadata()
    payload["effective_session_id"] = _record_session_id(record)
    return payload


def _record_session_id(record: Any) -> str | None:
    """Return the archived session id, including legacy agent-only records.

    Thin wrapper over :func:`qmb.jobs.session_manifest.effective_session_id`
    kept for callers that import it from cli.py.
    """
    from qmb.jobs.session_manifest import effective_session_id

    return effective_session_id(record)


def _record_agent_name(record: Any) -> str | None:
    if record.agent_context is None:
        return None
    return record.agent_context.name


def _record_created_at_utc(record: Any) -> datetime:
    created_at = record.created_at
    if created_at.tzinfo is None:
        return created_at.replace(tzinfo=UTC)
    return created_at.astimezone(UTC)


def _parse_job_date_filter(value: str | None) -> date | None:
    if value is None:
        return None
    raw = value.strip()
    if not _DATE_ONLY_PATTERN.fullmatch(raw):
        raise typer.BadParameter("Invalid --date value. Use YYYY-MM-DD.")
    try:
        return date.fromisoformat(raw)
    except ValueError as e:
        raise typer.BadParameter("Invalid --date value. Use YYYY-MM-DD.") from e


def _parse_job_datetime_filter(value: str | None, *, end_of_day: bool) -> datetime | None:
    if value is None:
        return None
    raw = value.strip()
    if not raw:
        raise typer.BadParameter("Date/time filters must not be empty.")
    if _DATE_ONLY_PATTERN.fullmatch(raw):
        try:
            parsed_date = date.fromisoformat(raw)
        except ValueError as e:
            raise typer.BadParameter(
                "Invalid date/time filter. Use YYYY-MM-DD or an ISO datetime."
            ) from e
        boundary_time = time.max if end_of_day else time.min
        return datetime.combine(parsed_date, boundary_time, tzinfo=UTC)
    try:
        parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError as e:
        raise typer.BadParameter(
            "Invalid date/time filter. Use YYYY-MM-DD or an ISO datetime."
        ) from e
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _any_contains(needle: str, *haystacks: str | None) -> bool:
    normalized = needle.casefold()
    return any(normalized in (haystack or "").casefold() for haystack in haystacks)


def _read_job_sql(record: Any) -> str:
    try:
        return record.query_path.read_text(encoding="utf-8")
    except OSError:
        return ""


@jobs_app.command("sessions")
def jobs_sessions(
    output_format: Annotated[
        str,
        typer.Option("--format", help="Output format: text or json"),
    ] = "text",
    limit: Annotated[
        int | None,
        typer.Option(
            "--limit",
            "-l",
            help="Cap the number of sessions returned (newest first).",
        ),
    ] = None,
) -> None:
    """List local qmb agent/session ids summarized from archived jobs."""
    from qmb.jobs.store import JobStore

    summaries = _session_summaries(JobStore())
    if limit is not None and limit >= 0:
        summaries = summaries[:limit]

    if output_format == "json":
        typer.echo(json.dumps(summaries, indent=2))
        return
    if output_format != "text":
        raise typer.BadParameter("Invalid format. Use text or json.")

    if not summaries:
        typer.echo("No qmb sessions found.")
        return

    from qmb.types import fmt_bytes

    for summary in summaries:
        agents = ",".join(summary["agents"]) if summary["agents"] else "-"
        typer.echo(
            f"{summary['latest']}  "
            f"{summary['count']:>4} jobs  "
            f"{fmt_bytes(summary['bytes_processed']):>10}  "
            f"{summary['session_id']}  "
            f"agents:{agents}"
        )


def _session_summaries(store: Any) -> list[dict[str, Any]]:
    """Build session summaries from manifests, falling back to a full scan.

    Manifests are the fast path (one file read per session). The fallback
    inside ``store.session_manifests()`` rebuilds from a full job scan when
    the ``sessions/`` directory is missing or empty, so this works on older
    archives that predate manifests.
    """
    summaries = [_manifest_to_summary(manifest) for manifest in store.session_manifests()]
    summaries.sort(key=lambda summary: summary["latest"] or "", reverse=True)
    return summaries


def _manifest_to_summary(manifest: Any) -> dict[str, Any]:
    """Convert a SessionManifest to the legacy jobs-sessions JSON shape."""
    return {
        "session_id": manifest.session_id,
        "count": manifest.count,
        "bytes_processed": manifest.bytes_processed,
        "first": manifest.first,
        "latest": manifest.latest,
        "agents": list(manifest.agents),
        "tasks": list(manifest.tasks),
        "cwds": list(manifest.cwds),
    }


@jobs_app.command("show")
def jobs_show(
    job_id: Annotated[str, typer.Argument(help="Full or partial qmb job ID")],
    output_format: Annotated[
        str,
        typer.Option("--format", help="Output format: text or json"),
    ] = "text",
    destination: Annotated[
        str | None,
        typer.Option(
            "--destination",
            help="Remote archive URI used if the job is missing locally.",
        ),
    ] = None,
) -> None:
    """Show metadata for a qmb job.

    Imports the job from the configured remote archive first if it is missing
    locally.
    """
    record = _load_job_or_exit(job_id, destination=destination)
    if output_format == "json":
        typer.echo(json.dumps(record.to_metadata(), indent=2))
        return
    if output_format != "text":
        raise typer.BadParameter("Invalid format. Use text or json.")

    typer.echo(f"Job: {record.qmb_job_id}")
    typer.echo(f"Created: {record.created_at.isoformat()}")
    typer.echo(f"Source: {record.source.label}")
    typer.echo(f"Engine: {record.engine.name}")
    if record.engine.job_id:
        typer.echo(f"Engine job: {record.engine.job_id}")
    typer.echo(f"Rows: {record.total_rows:,}")
    typer.echo(f"Bytes processed: {record.bytes_processed:,}")


@jobs_app.command("sql")
def jobs_sql(
    job_id: Annotated[str, typer.Argument(help="Full or partial qmb job ID")],
    destination: Annotated[
        str | None,
        typer.Option(
            "--destination",
            help="Remote archive URI used if the job is missing locally.",
        ),
    ] = None,
) -> None:
    """Print the archived resolved SQL for a qmb job.

    Imports the job from the configured remote archive first if it is missing
    locally.
    """
    record = _load_job_or_exit(job_id, destination=destination)
    typer.echo(record.query_path.read_text(encoding="utf-8"))


@jobs_app.command("open")
def jobs_open(
    job_id: Annotated[str, typer.Argument(help="Full or partial qmb job ID")],
    page_size: Annotated[
        int,
        typer.Option("--page-size", help="Rows per page in TUI"),
    ] = 200,
    destination: Annotated[
        str | None,
        typer.Option(
            "--destination",
            help="Remote archive URI used if the job is missing locally.",
        ),
    ] = None,
) -> None:
    """Open an archived qmb job's row preview in the TUI.

    Imports the job from the configured remote archive first if it is missing
    locally.
    """
    from qmb.jobs.result_source import JsonlPreviewResultSource
    from qmb.tui.app import QueryResultApp
    from qmb.types import QueryResultHandle

    record = _load_job_or_exit(job_id, destination=destination)
    source = JsonlPreviewResultSource.from_job(record)
    schema = record.schema or []
    handle = QueryResultHandle(
        job_id=record.qmb_job_id,
        project=record.engine.project or "",
        location=record.engine.location or "",
        destination_table="",
        schema=[field.to_mapping() for field in schema],
        total_rows=source.total_rows,
        bytes_processed=record.bytes_processed,
        execution_seconds=record.execution_seconds,
    )
    tui = QueryResultApp(
        bq_client=None,
        handle=handle,
        source_label=f"archive: {record.qmb_job_id}",
        resolved_sql=record.query_path.read_text(encoding="utf-8"),
        page_size=page_size,
        result_source=source,
    )
    tui.run()


@jobs_app.command("paths")
def jobs_paths(
    job_id: Annotated[str, typer.Argument(help="Full or partial qmb job ID")],
    output_format: Annotated[
        str,
        typer.Option("--format", help="Output format: text or json"),
    ] = "text",
    destination: Annotated[
        str | None,
        typer.Option(
            "--destination",
            help="Remote archive URI used if the job is missing locally.",
        ),
    ] = None,
) -> None:
    """Print artifact paths for a qmb job.

    Imports the job from the configured remote archive first if it is missing
    locally.
    """
    record = _load_job_or_exit(job_id, destination=destination)
    paths = record.artifact_paths()
    if output_format == "json":
        typer.echo(json.dumps(paths, indent=2))
        return
    if output_format != "text":
        raise typer.BadParameter("Invalid format. Use text or json.")

    for name, path in paths.items():
        typer.echo(f"{name}: {path}")


@jobs_app.command("export")
def jobs_export(
    job_id: Annotated[
        str | None,
        typer.Argument(help="Full or partial qmb job ID to publish."),
    ] = None,
    session_id: Annotated[
        str | None,
        typer.Option("--session-id", "--session", help="Publish all jobs in this session."),
    ] = None,
    destination: Annotated[
        str | None,
        typer.Option(
            "--destination",
            help=(
                "Remote archive destination URI. Required when QMB_REMOTE_ARCHIVE_URI "
                "and ~/.qmb/config.toml are unset."
            ),
        ),
    ] = None,
    output_format: Annotated[
        str,
        typer.Option("--format", help="Output format: text or json"),
    ] = "text",
) -> None:
    """Publish local qmb job archives to remote storage."""
    if (job_id is None) == (session_id is None):
        raise typer.BadParameter("Provide exactly one of JOB_ID or --session-id.")

    from qmb.config import remote_archive_preview_rows, remote_archive_uri
    from qmb.jobs.remote import RemoteArchiveError, get_remote_archive
    from qmb.jobs.store import JobStore, JobStoreError

    try:
        store = JobStore()
        records = (
            [store.read(job_id)]
            if job_id is not None
            else _records_for_session(store, session_id)
        )
        resolved_destination = remote_archive_uri(destination)
        if resolved_destination is None:
            raise typer.BadParameter(
                "Remote archive destination is not configured. Set --destination, "
                "QMB_REMOTE_ARCHIVE_URI, or [remote_archive].uri in ~/.qmb/config.toml."
            )
        remote = get_remote_archive(resolved_destination)
        results = [
            remote.export_job(record, preview_rows=remote_archive_preview_rows()).to_mapping()
            for record in records
        ]
    except (JobStoreError, RemoteArchiveError) as e:
        raise typer.BadParameter(str(e)) from e
    _print_remote_archive_results(
        results,
        destination=resolved_destination,
        output_format=output_format,
        verb="exported",
    )


@jobs_app.command("import")
def jobs_import(
    job_id: Annotated[
        str | None,
        typer.Argument(help="Full or partial remote qmb job ID to import."),
    ] = None,
    session_id: Annotated[
        str | None,
        typer.Option("--session-id", "--session", help="Import all jobs in this session."),
    ] = None,
    destination: Annotated[
        str | None,
        typer.Option(
            "--destination",
            help=(
                "Remote archive destination URI. Required when QMB_REMOTE_ARCHIVE_URI "
                "and ~/.qmb/config.toml are unset."
            ),
        ),
    ] = None,
    overwrite: Annotated[
        bool,
        typer.Option("--overwrite", help="Replace an existing local job archive."),
    ] = False,
    output_format: Annotated[
        str,
        typer.Option("--format", help="Output format: text or json"),
    ] = "text",
) -> None:
    """Import remote qmb job archives into the local job store."""
    if (job_id is None) == (session_id is None):
        raise typer.BadParameter("Provide exactly one of JOB_ID or --session-id.")

    from qmb.config import remote_archive_uri
    from qmb.jobs.remote import RemoteArchiveError, get_remote_archive
    from qmb.jobs.store import JobStore

    try:
        resolved_destination = remote_archive_uri(destination)
        if resolved_destination is None:
            raise typer.BadParameter(
                "Remote archive destination is not configured. Set --destination, "
                "QMB_REMOTE_ARCHIVE_URI, or [remote_archive].uri in ~/.qmb/config.toml."
            )
        remote = get_remote_archive(resolved_destination)
        store = JobStore()
        if job_id is not None:
            results = [remote.import_job(job_id, store, overwrite=overwrite).to_mapping()]
        else:
            results = [
                result.to_mapping()
                for result in remote.import_session(session_id, store, overwrite=overwrite)
            ]
    except RemoteArchiveError as e:
        raise typer.BadParameter(str(e)) from e
    _print_remote_archive_results(
        results,
        destination=resolved_destination,
        output_format=output_format,
        verb="imported",
    )


def _records_for_session(store: Any, session_id: str | None) -> list[Any]:
    job_ids = store.list_session_job_ids(session_id) if session_id else []
    if not job_ids:
        raise typer.BadParameter(f"No local qmb jobs found for session: {session_id}")
    records = [store.read(job_id) for job_id in job_ids]
    records.sort(key=lambda record: record.created_at)
    return records


def _print_remote_archive_results(
    results: list[dict[str, Any]],
    *,
    destination: str,
    output_format: str,
    verb: str,
) -> None:
    payload = {
        "destination": destination,
        "count": len(results),
        "jobs": results,
    }
    if output_format == "json":
        typer.echo(json.dumps(payload, indent=2))
        return
    if output_format != "text":
        raise typer.BadParameter("Invalid format. Use text or json.")
    for result in results:
        uri = f" {result['uri']}" if result.get("uri") else ""
        typer.echo(f"{verb} {result['qmb_job_id']} status:{result['status']}{uri}")


def _try_import_remote_session(session_id: str, store: Any, *, destination: str | None) -> None:
    from qmb.config import remote_archive_uri
    from qmb.jobs.remote import get_remote_archive

    resolved_destination = remote_archive_uri(destination)
    if resolved_destination is None:
        return

    try:
        typer.echo(
            f"qmb: importing remote session {session_id} from {resolved_destination}",
            err=True,
        )
        remote = get_remote_archive(resolved_destination)
        remote.import_session(session_id, store, overwrite=False)
    except Exception:
        # Listing remains a local command when the remote session is absent or
        # unavailable. Point lookups surface remote failures more explicitly.
        return


def _load_job_or_exit(job_id: str, *, destination: str | None = None):
    from qmb.config import remote_archive_uri
    from qmb.jobs.remote import get_remote_archive
    from qmb.jobs.store import AmbiguousJobIdError, JobNotFoundError, JobStore

    store = JobStore()
    try:
        return store.read(job_id)
    except JobNotFoundError as local_error:
        resolved_destination = remote_archive_uri(destination)
        if resolved_destination is None:
            raise typer.BadParameter(
                f"{local_error}. Remote lookup is not configured; set --destination, "
                "QMB_REMOTE_ARCHIVE_URI, or [remote_archive].uri in ~/.qmb/config.toml."
            ) from local_error
        try:
            typer.echo(
                f"qmb: importing remote job {job_id} from {resolved_destination}",
                err=True,
            )
            remote = get_remote_archive(resolved_destination)
            result = remote.import_job(job_id, store, overwrite=False)
            return store.read(result.qmb_job_id)
        except Exception as remote_error:
            raise typer.BadParameter(
                f"{local_error}. Remote lookup failed: {remote_error}"
            ) from remote_error
        except JobNotFoundError as imported_error:
            raise typer.BadParameter(str(imported_error)) from imported_error
    except AmbiguousJobIdError as e:
        raise typer.BadParameter(str(e)) from e


@app.command()
def describe(
    target: Annotated[
        str,
        typer.Argument(
            help=(
                "A dataset (``my_dataset``) or fully-qualified table "
                "(``my_dataset.my_table``) to inspect."
            ),
        ),
    ],
    project: Annotated[
        str | None,
        typer.Option("--project", help="GCP project ID"),
    ] = None,
    location: Annotated[
        str | None,
        typer.Option("--location", help="BigQuery location (e.g. US, EU)"),
    ] = None,
) -> None:
    """Print dataset or table metadata as JSON.

    The output mirrors the BigQuery REST API representation: schema,
    partitioning, clustering, sizes, timestamps, labels, descriptions,
    and so on. For a dataset target, a top-level ``tables`` array lists
    the table ids inside it (sorted alphabetically, case-insensitive).
    Use this in place of ``bq show --format prettyjson``.
    """
    from qmb.bigquery.catalog import (
        get_dataset_metadata,
        get_table_metadata,
        list_dataset_tables,
    )
    from qmb.bigquery.client import get_client

    client = get_client(project, location)

    # ``dataset`` vs ``dataset.table`` — colons are also accepted as a
    # convenience (``project:dataset.table`` is BigQuery's own shorthand).
    normalized = target.replace(":", ".")
    parts = normalized.split(".")
    if len(parts) == 1:
        dataset = get_dataset_metadata(client, parts[0])
        payload = {
            "kind": "dataset",
            "dataset": dataset.to_api_repr(),
            "tables": list(list_dataset_tables(client, parts[0])),
        }
    elif len(parts) == 2:
        table = get_table_metadata(client, parts[0], parts[1])
        payload = {"kind": "table", "table": table.to_api_repr()}
    elif len(parts) == 3:
        # project.dataset.table — honor it but require the configured
        # client's project for now; cross-project describe is out of scope.
        table = get_table_metadata(client, parts[1], parts[2])
        payload = {"kind": "table", "table": table.to_api_repr()}
    else:
        raise typer.BadParameter(
            f"Cannot parse target {target!r}. "
            "Use 'dataset' or 'dataset.table'."
        )

    typer.echo(json.dumps(payload))


@app.command()
def browse(
    pattern: Annotated[
        str | None,
        typer.Argument(
            help=(
                "Optional dataset/table pattern (fuzzy match or glob, "
                "e.g. 'analytics_*'). Omit to list all datasets."
            ),
        ),
    ] = None,
    project: Annotated[
        str | None,
        typer.Option("--project", help="GCP project ID"),
    ] = None,
    location: Annotated[
        str | None,
        typer.Option("--location", help="BigQuery location (e.g. US, EU)"),
    ] = None,
    tui: Annotated[
        bool,
        typer.Option(
            "--tui",
            "-t",
            help="Open the interactive browser pane instead of printing JSON",
        ),
    ] = False,
    time_steps: Annotated[
        bool,
        typer.Option(
            "--time",
            help="Print per-step timings to stderr (does not affect stdout JSON).",
        ),
    ] = False,
    workers: Annotated[
        int,
        typer.Option(
            "--workers",
            help="Concurrent threads used to fetch per-dataset tables (default: 8).",
            min=1,
            max=128,
        ),
    ] = 8,
    legacy_list_tables: Annotated[
        bool,
        typer.Option(
            "--legacy-list-tables",
            help=(
                "Use the old per-dataset list_tables fan-out instead of the "
                "default INFORMATION_SCHEMA.TABLES path. Useful when the "
                "calling user lacks bigquery.jobs.create permission."
            ),
        ),
    ] = False,
    refresh_regions: Annotated[
        bool,
        typer.Option(
            "--refresh-regions",
            help=(
                "Ignore the cached region list for this project and rediscover "
                "from list_datasets. Has no effect with --legacy-list-tables."
            ),
        ),
    ] = False,
) -> None:
    """Inspect datasets and tables in the active project.

    Prints a JSON list of matches to stdout by default. With a positional
    ``pattern`` (fuzzy or glob, e.g. ``'analytics_*'``) the output is
    filtered to matching datasets and tables; without one it lists every
    dataset id. Pass ``-t`` / ``--tui`` to open the interactive Textual
    browser pane instead.
    """
    from qmb.bigquery.client import get_client

    client = get_client(project, location)

    if not tui:
        _browse_print_json(
            client,
            pattern,
            project,
            time_steps=time_steps,
            workers=workers,
            legacy_list_tables=legacy_list_tables,
            location=location,
            refresh_regions=refresh_regions,
        )
        return

    from qmb.tui.app import QueryResultApp
    from qmb.types import QueryResultHandle

    app_instance = QueryResultApp(
        bq_client=client,
        handle=QueryResultHandle(
            job_id="",
            project=client.project or project or "",
            location=location or getattr(client, "location", None) or "",
            destination_table="",
            schema=[],
            total_rows=0,
        ),
        source_label="browser",
        page_size=200,
        start_in_browser=True,
        browser_only=True,
    )
    app_instance.run()


def _browse_print_json(
    client: Any,
    pattern: str | None,
    project: str | None,
    *,
    time_steps: bool = False,
    workers: int = 8,
    legacy_list_tables: bool = False,
    location: str | None = None,
    refresh_regions: bool = False,
) -> None:
    """Print a JSON representation of the catalog (optionally filtered).

    When ``time_steps`` is true, print per-step wall-clock timings to
    stderr. stdout JSON is unchanged so ``| jq`` still works.
    """
    import sys
    import time as _time

    from qmb.bigquery.catalog import build_table_index, list_dataset_ids
    from qmb.bigquery.catalog_search import filter_browser_matches

    def _stamp(label: str, seconds: float, extra: str = "") -> None:
        if not time_steps:
            return
        suffix = f"  {extra}" if extra else ""
        print(f"[time] {label}: {seconds * 1000:8.1f} ms{suffix}", file=sys.stderr)

    t0 = _time.perf_counter()

    if pattern is None:
        # Cheap path: just the dataset list, no per-dataset table fetch.
        dataset_ids = list_dataset_ids(client)
        _stamp("list_dataset_ids", _time.perf_counter() - t0, f"n={len(dataset_ids)}")
        payload = {
            "project": getattr(client, "project", None) or project,
            "datasets": dataset_ids,
        }
        typer.echo(json.dumps(payload))
        return

    if legacy_list_tables:
        t_ds = _time.perf_counter()
        dataset_ids = list_dataset_ids(client)
        _stamp("list_dataset_ids", _time.perf_counter() - t_ds, f"n={len(dataset_ids)}")

        t1 = _time.perf_counter()
        tables_by_dataset = build_table_index(
            client, dataset_ids, max_workers=workers
        )
        total_tables = sum(len(v) for v in tables_by_dataset.values())
        _stamp(
            "build_table_index",
            _time.perf_counter() - t1,
            f"datasets={len(tables_by_dataset)} tables={total_tables} workers={workers}",
        )
    else:
        from qmb.bigquery.catalog_information_schema import (
            list_tables_via_information_schema,
        )

        # If --location is given, pin to a single region (skip
        # auto-discovery). Otherwise discover regions from list_datasets
        # so multi-region projects are handled correctly.
        explicit_location = location or getattr(client, "location", None)
        pinned_locations = [explicit_location] if explicit_location else None

        t1 = _time.perf_counter()
        tables_by_dataset = list_tables_via_information_schema(
            client,
            project=project,
            locations=pinned_locations,
            refresh_cache=refresh_regions,
        )
        total_tables = sum(len(v) for v in tables_by_dataset.values())
        _stamp(
            "list_tables_via_information_schema",
            _time.perf_counter() - t1,
            f"datasets={len(tables_by_dataset)} tables={total_tables} "
            f"locations={pinned_locations or 'auto'}",
        )
        dataset_ids = sorted(tables_by_dataset.keys(), key=str.lower)

    t2 = _time.perf_counter()
    matches = filter_browser_matches(dataset_ids, tables_by_dataset, pattern)
    _stamp(
        "filter_browser_matches",
        _time.perf_counter() - t2,
        f"matches={len(matches)}",
    )

    payload = {
        "project": getattr(client, "project", None) or project,
        "pattern": pattern,
        "matches": [
            {"dataset_id": m.dataset_id, "tables": list(m.tables)} for m in matches
        ],
    }
    typer.echo(json.dumps(payload))
    _stamp("total", _time.perf_counter() - t0)


def _execute(
    request: QueryRequest,
    *,
    output_format: Format,
    publish: bool = False,
    destination: str | None = None,
) -> None:
    """Run the application pipeline and render the result via a formatter.

    ``output_format`` is the parsed ``--format`` value; the CLI dispatches
    a single formatter from :func:`qmb.formatters.get_formatter`. ``run``
    defaults to :class:`Format.JSON` so headless / agent use “just works”.
    """
    from qmb.application.pipeline import run_query_pipeline
    from qmb.dbt.integration import DbtSqlResolver
    from qmb.jobs.store import JobStore
    from qmb.sql.resolver import PlainSqlResolver

    # The CLI is the composition root: it decides which resolvers exist
    # and in what order. Order matters — the first resolver whose
    # ``can_resolve`` returns True wins.
    resolvers = [DbtSqlResolver(), PlainSqlResolver()]

    outcome = run_query_pipeline(
        request,
        resolvers=resolvers,
        job_store=JobStore(),
        ignore_archive_errors=True,
    )
    if publish:
        outcome = _publish_outcome_archive(outcome, destination=destination)
    _render_outcome(outcome, request, output_format=output_format)


def _publish_outcome_archive(
    outcome: ExecutionOutcome,
    *,
    destination: str | None = None,
) -> ExecutionOutcome:
    """Publish the archived job for a run and attach machine-readable status."""
    if outcome.dry_run:
        return replace(
            outcome,
            remote_archive={
                "status": "skipped",
                "reason": "dry_run",
                "destination": None,
                "jobs": [],
            },
        )
    if outcome.archived_job is None:
        return replace(
            outcome,
            remote_archive={
                "status": "failed",
                "reason": "local_archive_missing",
                "destination": None,
                "jobs": [],
            },
        )

    from qmb.config import remote_archive_preview_rows, remote_archive_uri
    from qmb.jobs.remote import get_remote_archive

    try:
        resolved_destination = remote_archive_uri(destination)
        if resolved_destination is None:
            raise ValueError(
                "Remote archive destination is not configured. Set --destination, "
                "QMB_REMOTE_ARCHIVE_URI, or [remote_archive].uri in ~/.qmb/config.toml."
            )
        result = get_remote_archive(resolved_destination).export_job(
            outcome.archived_job,
            preview_rows=remote_archive_preview_rows(),
        )
        payload = {
            "status": result.status,
            "destination": resolved_destination,
            "jobs": [result.to_mapping()],
            "error": None,
        }
    except Exception as exc:
        payload = {
            "status": "failed",
            "destination": resolved_destination,
            "jobs": [],
            "error": f"{type(exc).__name__}: {exc}",
        }
    return replace(outcome, remote_archive=payload)


def _render_outcome(
    outcome: ExecutionOutcome,
    request: QueryRequest,
    *,
    output_format: Format,
) -> None:
    """Dispatch the outcome to the formatter selected by ``--format``."""
    from qmb.formatters import get_formatter

    get_formatter(output_format).render_run(outcome, request)


if __name__ == "__main__":
    app()
