import type { AgentFields, ArchiveTagged, IndexResponse, Origin } from "./types";

/**
 * Merge the independently-fetched local half and each remote archive's half
 * into the combined shape the rest of the UI expects (mirrors the server's
 * `?scope=`-less `/api/index` assembly in
 * `src/qmb/web/server.py::JobIndexCache`, but runs client-side per the
 * two-phase load: local renders immediately, each archive merges in when it
 * arrives).
 *
 * A remote item whose id is also local folds into the local entry (local
 * fields win), tagged `"both"` with the archive listed in `also_in`. Remote
 * items are otherwise kept per archive, so the same id in two archives
 * stays two entries. With no archive loaded yet, the local-only view is
 * returned unchanged.
 */
export function mergeIndex(
  local: IndexResponse | null,
  remotes: IndexResponse[],
): IndexResponse | null {
  if (!local) return null;
  if (remotes.length === 0) return local;
  const generatedAt = [local, ...remotes]
    .map((payload) => payload.generated_at)
    .reduce((latest, at) => (at > latest ? at : latest));
  const staleCount = remotes.reduce((sum, payload) => sum + (payload.index_stale ?? 0), 0);
  return {
    generated_at: generatedAt,
    jobs: mergeIntoLocal(
      local.jobs,
      remotes.map((payload) => payload.jobs),
      (job) => job.qmb_job_id,
    ),
    sessions: mergeIntoLocal(
      local.sessions,
      remotes.map((payload) => payload.sessions),
      (session) => session.session_id,
    ),
    archives: remotes.flatMap((payload) => payload.archives ?? []),
    index_stale: staleCount || undefined,
  };
}

function mergeIntoLocal<T extends ArchiveTagged & AgentFields & { origin: Origin }>(
  localItems: T[],
  remoteLists: T[][],
  keyOf: (item: T) => string,
): T[] {
  const merged = localItems.map((item) => ({ ...item }));
  const localByKey = new Map(merged.map((item) => [keyOf(item), item]));
  for (const remoteItems of remoteLists) {
    for (const item of remoteItems) {
      const localItem = localByKey.get(keyOf(item));
      if (!localItem) {
        merged.push(item);
        continue;
      }
      localItem.origin = "both";
      localItem.also_in = [...(localItem.also_in ?? []), item.archive];
      localItem.agent_name ??= item.agent_name;
      localItem.user ??= item.user;
      localItem.task ??= item.task;
    }
  }
  return merged;
}
