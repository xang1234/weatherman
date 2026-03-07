# Weatherman — Development Environment

## Python (Backend)

- **Package manager**: `uv` (not pip/poetry)
- **Python version**: 3.14 (managed by uv, see `.venv/`)
- **Run commands**: Always use `uv run` prefix
  ```bash
  uv run pytest              # run tests
  uv run pytest tests/test_foo.py -v  # single test file
  uv run python -m weatherman.foo     # run a module
  ```
- **Add dependencies**: `uv add <package>` (or `uv add --dev <package>` for dev deps)
- **Lock file**: `uv.lock` — committed to git, do not edit manually

## Node.js (Frontend)

- **Directory**: `frontend/`
- **Node version**: v22 via nvm (system default is v14 which is too old)
- **Always prefix Node commands**:
  ```bash
  export PATH="$HOME/.nvm/versions/node/v22.18.0/bin:$PATH"
  ```
- **Package manager**: npm
- **Common commands**:
  ```bash
  npx tsc -b --noEmit   # type-check
  npx vite build         # production build
  npx vite dev           # dev server
  ```
- **Path aliases**: `@/` maps to `frontend/src/` (configured in tsconfig + vite)

## Testing

- Backend: `uv run pytest` (pytest with coverage plugin available)
- Frontend: TypeScript compilation (`npx tsc -b --noEmit`) + Vite build
- Always run relevant tests before closing a bead
