interface Props {
  message: string;
  onDismiss: () => void;
  /** `warning` is the quieter, non-blocking variant (e.g. one archive failed). */
  tone?: "error" | "warning";
}

export function Banner({ message, onDismiss, tone = "error" }: Props) {
  return (
    <div class={tone === "warning" ? "banner banner--warning" : "banner"}>
      <span>{message}</span>
      <button type="button" class="banner__dismiss" onClick={onDismiss} aria-label="Dismiss">
        ✕
      </button>
    </div>
  );
}
