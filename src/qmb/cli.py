"""CLI entrypoint for qmb."""

from __future__ import annotations

import re
from pathlib import Path
from typing import TYPE_CHECKING, Annotated, Any

import typer
from rich.console import Console
from rich.panel import Panel

from qmb.types import fmt_bytes

if TYPE_CHECKING:
    from qmb.types import QueryRequest, ResolvedQuery

app = typer.Typer(
    name="qmb",
    help="Query Me Big – Run BigQuery queries with a Textual TUI, dbt support, and export.",
    no_args_is_help=True,
)
console = Console()

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
    no_tui: Annotated[
        bool,
        typer.Option("--no-tui", help="Skip TUI, just export or print summary"),
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
    browser_only: Annotated[
        bool,
        typer.Option(
            "--browser-only",
            "--browse",
            help="Open the dataset/table browser without running a query",
        ),
    ] = False,

) -> None:
    """Run a BigQuery query with optional dbt model resolution."""
    from qmb.types import ExportFormat, InputMode, QueryRequest

    # Validate mutually exclusive inputs
    inputs = sum(x is not None for x in [query, file, model])
    if browser_only and inputs > 0:
        raise typer.BadParameter(
            "--browser-only cannot be combined with query, --file, or --model."
        )
    if browser_only and (export or out or no_tui or dry_run or max_bytes_billed or where):
        raise typer.BadParameter(
            "--browser-only cannot be combined with export, --no-tui, --dry-run, "
            "--max-bytes-billed, or --where."
        )
    if not browser_only and inputs == 0:
        raise typer.BadParameter("Provide one of query, --file, or --model.")
    if inputs > 1:
        raise typer.BadParameter("Provide only one of query, --file, or --model.")

    # Determine mode
    if browser_only:
        mode = InputMode.BROWSER
    elif query is not None:
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

    _execute(request)


def _execute(request: QueryRequest) -> None:
    """Core execution pipeline."""
    from qmb.bigquery.client import get_client
    from qmb.bigquery.executor import execute_query
    from qmb.bigquery.exporters import export_results
    from qmb.tui.app import QueryResultApp
    from qmb.types import QueryResultHandle

    client = get_client(request.project, request.location)

    if request.mode.name == "BROWSER":
        tui = QueryResultApp(
            bq_client=client,
            handle=QueryResultHandle(
                job_id="",
                project=client.project or request.project or "",
                location=request.location or getattr(client, "location", None) or "",
                destination_table="",
                schema=[],
                total_rows=0,
            ),
            source_label="browser",
            page_size=request.page_size,
            start_in_browser=True,
            browser_only=True,
        )
        tui.run()
        return

    # Step 1: Resolve SQL
    resolved = _resolve_sql(request)

    # Step 1.5: Apply --where clause
    if request.where:
        from qmb.types import ResolvedQuery

        resolved = ResolvedQuery(
            sql=f"SELECT * FROM ({resolved.sql}) __qmb WHERE {request.where}",
            source_label=resolved.source_label,
        )

    # Step 2: Execute
    if request.dry_run:
        handle = execute_query(
            client, resolved, dry_run=True, max_bytes_billed=request.max_bytes_billed
        )
        console.print(Panel(resolved.sql, title="Resolved SQL (dry run)", border_style="cyan"))
        console.print(f"[cyan]Estimated:[/cyan] {fmt_bytes(handle.bytes_processed)}")
        return

    console.print(f"[dim]Source: {resolved.source_label}[/dim]")
    console.print("[dim]Executing query...[/dim]")

    handle = execute_query(client, resolved, max_bytes_billed=request.max_bytes_billed)

    console.print(
        f"[green]✓[/green] {handle.total_rows:,} rows · "
        f"{fmt_bytes(handle.bytes_processed)} processed · "
        f"Job: {handle.job_id}"
    )

    # Step 3: Export if requested
    if request.export_format and request.export_path:
        console.print(f"[dim]Exporting to {request.export_path}...[/dim]")
        count = export_results(client, handle, request.export_format, request.export_path)
        console.print(
            f"[green]✓[/green] Exported {count:,} rows to {request.export_path}"
        )

    # Step 4: TUI or exit
    if request.no_tui:
        return

    if handle.total_rows == 0:
        console.print("[yellow]No rows to display.[/yellow]")
        return

    tui = QueryResultApp(
        bq_client=client,
        handle=handle,
        source_label=resolved.source_label,
        resolved_sql=resolved.sql,
        page_size=request.page_size,
    )
    tui.run()


def _resolve_sql(request: QueryRequest) -> ResolvedQuery:
    """Resolve the SQL from the request."""
    from qmb.types import InputMode

    if request.mode == InputMode.SQL:
        from qmb.sql.loader import load_sql

        return load_sql(request)

    if request.mode == InputMode.FILE:
        from qmb.sql.loader import load_sql

        resolved = load_sql(request)

        if request.resolve_dbt:
            from qmb.dbt.manifest import discover_manifest_path, load_manifest
            from qmb.dbt.resolver import resolve_file_sql, resolve_file_to_model
            from qmb.sql.loader import normalize_sql
            from qmb.types import ResolvedQuery

            manifest_path = request.manifest_path or discover_manifest_path()
            index = load_manifest(manifest_path)

            # Try to match file to a compiled manifest node first
            if request.file_path:
                node = resolve_file_to_model(str(request.file_path), index)
                if node:
                    sql = node.compiled_code
                    if sql:
                        console.print(
                            f"[dim]Matched manifest node: {node.unique_id}[/dim]"
                        )
                        return ResolvedQuery(
                            sql=normalize_sql(sql),
                            source_label=f"model: {node.name} ({node.unique_id})",
                        )
                    # No compiled_code — use raw_code with config stripped
                    if node.raw_code:
                        from qmb.dbt.resolver import strip_config_blocks

                        console.print(
                            f"[dim]Matched {node.unique_id} (no compiled_code, "
                            "resolving from raw SQL)[/dim]"
                        )
                        return resolve_file_sql(
                            strip_config_blocks(node.raw_code),
                            index,
                            request.variables,
                            source_label=f"model: {node.name} ({node.unique_id})",
                        )

            return resolve_file_sql(
                resolved.sql,
                index,
                request.variables,
                source_label=resolved.source_label,
            )

        return resolved

    if request.mode == InputMode.MODEL:
        assert request.manifest_path is not None
        assert request.model_name is not None

        from qmb.dbt.manifest import load_manifest
        from qmb.dbt.resolver import resolve_model_query

        index = load_manifest(request.manifest_path)
        return resolve_model_query(request.model_name, index, request.variables)

    if request.mode == InputMode.BROWSER:
        raise typer.BadParameter("Browser mode does not resolve SQL.")

    raise typer.BadParameter(f"Unknown mode: {request.mode}")


if __name__ == "__main__":
    app()
