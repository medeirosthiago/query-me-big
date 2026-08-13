# qmb web frontend

Vite + Preact + TypeScript. `npm install`, then:

- `npm run dev` — dev server with a `/api` proxy to `http://127.0.0.1:8850` (run `qmb web` separately)
- `npm run typecheck` — `tsc --noEmit`
- `npm run build` — builds into `../src/qmb/web/static/`, served by `qmb web`
