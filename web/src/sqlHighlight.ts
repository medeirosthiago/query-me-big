export type SqlTokenType =
  | "keyword"
  | "string"
  | "number"
  | "comment"
  | "identifier"
  | "punctuation"
  | "whitespace";

export interface SqlToken {
  text: string;
  type: SqlTokenType;
}

// A pragmatic set covering standard SQL + common BigQuery extensions. Not
// exhaustive by design — this is a lightweight highlighter, not a parser.
const KEYWORDS = new Set(
  `
  SELECT FROM WHERE JOIN LEFT RIGHT INNER OUTER FULL CROSS ON GROUP BY ORDER HAVING
  LIMIT OFFSET AS WITH INSERT INTO VALUES UPDATE SET DELETE MERGE CREATE OR REPLACE
  TEMP TEMPORARY TABLE VIEW MATERIALIZED FUNCTION PROCEDURE DROP ALTER ADD COLUMN
  UNION ALL DISTINCT AND NOT NULL IS IN EXISTS BETWEEN LIKE ILIKE CASE WHEN THEN
  ELSE END CAST SAFE_CAST PARTITION OVER WINDOW ASC DESC USING QUALIFY EXCEPT
  INTERSECT TRUE FALSE DEFAULT ARRAY STRUCT UNNEST RETURNS LANGUAGE OPTIONS DECLARE
  BEGIN COMMIT ROLLBACK IF ELSEIF WHILE LOOP FOR EACH LATERAL ROWS RANGE PRECEDING
  FOLLOWING CURRENT ROW UNBOUNDED INTERVAL EXTRACT
`
    .split(/\s+/)
    .filter(Boolean),
);

const TOKEN_RE = new RegExp(
  [
    String.raw`--[^\n]*`, // line comment
    String.raw`/\*[\s\S]*?\*/`, // block comment
    String.raw`'''[\s\S]*?'''`, // triple single-quoted string (BQ)
    String.raw`"""[\s\S]*?"""`, // triple double-quoted string (BQ)
    String.raw`'(?:\\.|[^'\\])*'`, // single-quoted string
    String.raw`"(?:\\.|[^"\\])*"`, // double-quoted string
    String.raw`\`(?:\\.|[^\`\\])*\``, // backtick-quoted identifier
    String.raw`\d+\.\d+(?:[eE][+-]?\d+)?|\d+(?:[eE][+-]?\d+)?`, // number
    String.raw`[A-Za-z_][A-Za-z0-9_]*`, // identifier/keyword
    String.raw`\s+`, // whitespace
    String.raw`.`, // fallback: single punctuation char
  ].join("|"),
  "gs",
);

export function tokenizeSql(sql: string): SqlToken[] {
  const tokens: SqlToken[] = [];
  for (const match of sql.matchAll(TOKEN_RE)) {
    const text = match[0];
    tokens.push({ text, type: classify(text) });
  }
  return tokens;
}

function classify(text: string): SqlTokenType {
  const first = text[0];
  if (first === undefined) return "punctuation";
  if (/\s/.test(first)) return "whitespace";
  if (text.startsWith("--") || text.startsWith("/*")) return "comment";
  if (first === "'" || first === '"' || first === "`") return "string";
  if (/[0-9]/.test(first)) return "number";
  if (/[A-Za-z_]/.test(first)) {
    return KEYWORDS.has(text.toUpperCase()) ? "keyword" : "identifier";
  }
  return "punctuation";
}
