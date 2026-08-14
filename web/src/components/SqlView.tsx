import { useState } from "preact/hooks";
import { tokenizeSql } from "../sqlHighlight";
import { CopyIcon } from "./CopyIcon";

export function SqlView({ sql }: { sql: string }) {
  const tokens = tokenizeSql(sql);
  const [copied, setCopied] = useState(false);

  async function copy() {
    try {
      await navigator.clipboard.writeText(sql);
      setCopied(true);
      setTimeout(() => setCopied(false), 1500);
    } catch {
      // Clipboard API unavailable (e.g. insecure context) — nothing to do.
    }
  }

  return (
    <div class="sql-view-wrap">
      <button
        type="button"
        class="sql-view__copy-btn"
        aria-label="Copy"
        title="Copy"
        onClick={copy}
      >
        <CopyIcon copied={copied} />
      </button>
      <pre class="sql-view">
        <code>
          {tokens.map((token, i) => (
            <span key={i} class={`sql-tok sql-tok--${token.type}`}>
              {token.text}
            </span>
          ))}
        </code>
      </pre>
    </div>
  );
}
