import type { IndexResponse, Origin } from "./types";

/**
 * Merge the independently-fetched local and remote index halves into the
 * combined shape the rest of the UI expects (mirrors the server's `?scope=`
 * -less `/api/index` assembly in `src/qmb/web/server.py::JobIndexCache`, but
 * runs client-side per the two-phase load: local renders immediately,
 * remote merges in when it arrives).
 *
 * Local fields win for entries present in both (tagged `"both"`). If
 * `remote` hasn't loaded yet, the local-only view is returned unchanged —
 * its entries are already tagged `"local"` by the server.
 */
export function mergeIndex(
  local: IndexResponse | null,
  remote: IndexResponse | null,
): IndexResponse | null {
  if (!local) return null;
  if (!remote) return local;
  return {
    generated_at: remote.generated_at > local.generated_at ? remote.generated_at : local.generated_at,
    jobs: mergeTagged(local.jobs, remote.jobs, (job) => job.qmb_job_id),
    sessions: mergeTagged(local.sessions, remote.sessions, (session) => session.session_id),
    remote_error: remote.remote_error,
    index_stale: remote.index_stale,
  };
}

function mergeTagged<T extends { origin: Origin }>(
  localItems: T[],
  remoteItems: T[],
  keyOf: (item: T) => string,
): T[] {
  const remoteByKey = new Map(remoteItems.map((item) => [keyOf(item), item]));
  const localKeys = new Set(localItems.map(keyOf));
  const merged = localItems.map((item) => ({
    ...item,
    origin: (remoteByKey.has(keyOf(item)) ? "both" : "local") as Origin,
  }));
  for (const [key, item] of remoteByKey) {
    if (!localKeys.has(key)) merged.push({ ...item, origin: "remote" as Origin });
  }
  return merged;
}
