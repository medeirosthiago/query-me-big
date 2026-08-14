interface Props {
  message: string;
  onDismiss: () => void;
}

export function Banner({ message, onDismiss }: Props) {
  return (
    <div class="banner">
      <span>{message}</span>
      <button type="button" class="banner__dismiss" onClick={onDismiss} aria-label="Dismiss">
        ✕
      </button>
    </div>
  );
}
