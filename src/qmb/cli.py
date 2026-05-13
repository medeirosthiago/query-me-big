"""CLI entrypoint for qmb."""

from __future__ import annotations

import json
import re
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


def _parse_vars(var_list: list[str] | None) -> dict[str, Any]:
    """Parse --var key=value pairs."""
    if not var_list:
        return {}
    variables: dict[str, Any] = {}
    for item in var_list:
        if "=" not in item:
            raise typer.BadParameter(f"Invalid --var format: '{item}'. Use key=value.")
        key, _, value = item.partition("=")
        variables[key.strip()] = _coerce_var_value(value.strip())
    return variables


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
    tui: Annotated[
        bool,
        typer.Option(
            "--tui",
            "-t",
            help="Open the interactive Textual TUI instead of printing JSON",
        ),
    ] = False,
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

    variables = _parse_vars(var)

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
    )

    _execute(request, output_format=selected_format)




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
    """
    from qmb.bigquery.client import get_client
    from qmb.bigquery.history import list_recent_queries

    client = get_client(project, location)
    entries = list_recent_queries(client, days=days, limit=limit)

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
    }


@jobs_app.command("list")
def jobs_list(
    output_format: Annotated[
        str,
        typer.Option("--format", help="Output format: text or json"),
    ] = "text",
) -> None:
    """List local qmb job archives."""
    from qmb.jobs.store import JobStore

    store = JobStore()
    records = store.list()
    if output_format == "json":
        typer.echo(json.dumps([record.to_metadata() for record in records], indent=2))
        return
    if output_format != "text":
        raise typer.BadParameter("Invalid format. Use text or json.")

    if not records:
        typer.echo("No local qmb jobs found.")
        return

    for record in records:
        typer.echo(
            f"{record.created_at:%Y-%m-%d %H:%M:%S}  "
            f"{record.qmb_job_id}  {record.source.label}  "
            f"{record.total_rows:,} rows"
        )


@jobs_app.command("show")
def jobs_show(
    job_id: Annotated[str, typer.Argument(help="Full or partial qmb job ID")],
    output_format: Annotated[
        str,
        typer.Option("--format", help="Output format: text or json"),
    ] = "text",
) -> None:
    """Show metadata for a local qmb job."""
    record = _load_job_or_exit(job_id)
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
) -> None:
    """Print the archived resolved SQL for a local qmb job."""
    record = _load_job_or_exit(job_id)
    typer.echo(record.query_path.read_text(encoding="utf-8"))


@jobs_app.command("open")
def jobs_open(
    job_id: Annotated[str, typer.Argument(help="Full or partial qmb job ID")],
    page_size: Annotated[
        int,
        typer.Option("--page-size", help="Rows per page in TUI"),
    ] = 200,
) -> None:
    """Open an archived qmb job preview in the TUI."""
    from qmb.jobs.result_source import JsonlPreviewResultSource
    from qmb.tui.app import QueryResultApp
    from qmb.types import QueryResultHandle

    record = _load_job_or_exit(job_id)
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
) -> None:
    """Print artifact paths for a local qmb job."""
    record = _load_job_or_exit(job_id)
    paths = record.artifact_paths()
    if output_format == "json":
        typer.echo(json.dumps(paths, indent=2))
        return
    if output_format != "text":
        raise typer.BadParameter("Invalid format. Use text or json.")

    for name, path in paths.items():
        typer.echo(f"{name}: {path}")


def _load_job_or_exit(job_id: str):
    from qmb.jobs.store import AmbiguousJobIdError, JobNotFoundError, JobStore

    try:
        return JobStore().read(job_id)
    except JobNotFoundError as e:
        raise typer.BadParameter(str(e)) from e
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
    and so on. Use this in place of ``bq show --format prettyjson``.
    """
    from qmb.bigquery.catalog import get_dataset_metadata, get_table_metadata
    from qmb.bigquery.client import get_client

    client = get_client(project, location)

    # ``dataset`` vs ``dataset.table`` — colons are also accepted as a
    # convenience (``project:dataset.table`` is BigQuery's own shorthand).
    normalized = target.replace(":", ".")
    parts = normalized.split(".")
    if len(parts) == 1:
        dataset = get_dataset_metadata(client, parts[0])
        payload = {"kind": "dataset", "dataset": dataset.to_api_repr()}
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
        _browse_print_json(client, pattern, project)
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


def _browse_print_json(client: Any, pattern: str | None, project: str | None) -> None:
    """Print a JSON representation of the catalog (optionally filtered)."""
    from qmb.bigquery.catalog import build_table_index, list_dataset_ids
    from qmb.bigquery.catalog_search import filter_browser_matches

    dataset_ids = list_dataset_ids(client)

    if pattern is None:
        # Cheap path: just the dataset list, no per-dataset table fetch.
        payload = {
            "project": getattr(client, "project", None) or project,
            "datasets": dataset_ids,
        }
        typer.echo(json.dumps(payload))
        return

    tables_by_dataset = build_table_index(client, dataset_ids)
    matches = filter_browser_matches(dataset_ids, tables_by_dataset, pattern)
    payload = {
        "project": getattr(client, "project", None) or project,
        "pattern": pattern,
        "matches": [
            {"dataset_id": m.dataset_id, "tables": list(m.tables)} for m in matches
        ],
    }
    typer.echo(json.dumps(payload))


def _execute(request: QueryRequest, *, output_format: Format) -> None:
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
    _render_outcome(outcome, request, output_format=output_format)


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
