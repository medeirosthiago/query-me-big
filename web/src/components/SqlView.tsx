import { tokenizeSql } from "../sqlHighlight";

export function SqlView({ sql }: { sql: string }) {
  const tokens = tokenizeSql(sql);
  return (
    <pre class="sql-view">
      <code>
        {tokens.map((token, i) => (
          <span key={i} class={`sql-tok sql-tok--${token.type}`}>
            {token.text}
          </span>
        ))}
      </code>
    </pre>
  );
}
