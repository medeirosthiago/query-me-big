import { useState } from "preact/hooks";
import { CopyIcon } from "./CopyIcon";

export function CopyLine({ command }: { command: string }) {
  const [copied, setCopied] = useState(false);

  async function copy() {
    try {
      await navigator.clipboard.writeText(command);
      setCopied(true);
      setTimeout(() => setCopied(false), 1500);
    } catch {
      // Clipboard API unavailable (e.g. insecure context) — nothing to do.
    }
  }

  return (
    <div class="copy-line">
      <code>{command}</code>
      <button
        type="button"
        class="copy-line__btn"
        aria-label="Copy"
        title="Copy"
        onClick={copy}
      >
        <CopyIcon copied={copied} />
      </button>
    </div>
  );
}
