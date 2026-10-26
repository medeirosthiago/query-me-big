import { archiveBadgeClass, archivesOf } from "../archives";
import type { ArchiveTagged } from "../types";

interface Props {
  item: ArchiveTagged;
  /** Name of the primary (read/write) archive, for its colour. */
  primary: string | null;
}

/** One badge per archive the item lives in: `LOCAL`, then archive names. */
export function ArchiveBadges({ item, primary }: Props) {
  return (
    <>
      {archivesOf(item).map((archive) => (
        <span key={archive} class={archiveBadgeClass(archive, primary)}>
          {archive}
        </span>
      ))}
    </>
  );
}
