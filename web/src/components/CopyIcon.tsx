export function CopyIcon({ copied }: { copied: boolean }) {
  const common = {
    width: 14,
    height: 14,
    viewBox: "0 0 14 14",
    fill: "none",
    stroke: "currentColor",
    "stroke-width": 1.3,
    "stroke-linecap": "round" as const,
    "stroke-linejoin": "round" as const,
    "aria-hidden": true,
  };
  if (copied) {
    return (
      <svg {...common}>
        <path d="M2.8 7.2l3 3 5.4-6.4" />
      </svg>
    );
  }
  return (
    <svg {...common}>
      <rect x="4.6" y="4.6" width="7.4" height="7.4" rx="1.2" />
      <path d="M9 4.6V2.8a1.2 1.2 0 0 0-1.2-1.2H2.8a1.2 1.2 0 0 0-1.2 1.2v5a1.2 1.2 0 0 0 1.2 1.2h1.8" />
    </svg>
  );
}
