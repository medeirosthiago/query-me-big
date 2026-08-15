/** Port of `qmb.types.fmt_bytes`: human-readable byte counts. */
export function fmtBytes(n: number | null | undefined): string {
  if (n === null || n === undefined) return "\u2014";
  if (n < 1024) return `${n} B`;
  let value = n;
  for (const unit of ["KB", "MB", "GB", "TB"]) {
    value /= 1024;
    if (value < 1024) return `${formatFixed1(value)} ${unit}`;
  }
  return `${formatFixed1(value)} PB`;
}

function formatFixed1(value: number): string {
  return value.toLocaleString(undefined, { minimumFractionDigits: 1, maximumFractionDigits: 1 });
}

export function fmtNumber(n: number): string {
  return n.toLocaleString();
}

/** Format an ISO timestamp as `YYYY-MM-DD HH:MM` in the local timezone. */
export function fmtDate(iso: string | null | undefined): string {
  if (!iso) return "\u2014";
  const date = new Date(iso);
  if (Number.isNaN(date.getTime())) return iso;
  const pad = (n: number) => String(n).padStart(2, "0");
  return (
    `${date.getFullYear()}-${pad(date.getMonth() + 1)}-${pad(date.getDate())} ` +
    `${pad(date.getHours())}:${pad(date.getMinutes())}`
  );
}

export function fmtSeconds(seconds: number | null | undefined): string {
  if (seconds === null || seconds === undefined) return "\u2014";
  if (seconds < 1) return `${Math.round(seconds * 1000)} ms`;
  return `${seconds.toLocaleString(undefined, { maximumFractionDigits: 1 })} s`;
}
