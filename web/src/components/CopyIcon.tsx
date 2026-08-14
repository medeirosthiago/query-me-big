import { Icon } from "./Icon";

export function CopyIcon({ copied }: { copied: boolean }) {
  return <Icon name={copied ? "check" : "copy"} />;
}
