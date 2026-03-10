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
- **Node version**: v22 via nvm (system node is v14, too old)
- **Running commands**: System node is v14 (too old). `export PATH` does not persist across shell calls and `npx` fails. Prefix every command with an inline `PATH=` override from `frontend/`:
  ```bash
  cd frontend
  PATH="/Users/admin/.nvm/versions/node/v22.18.0/bin:/usr/bin:/bin" ./node_modules/.bin/tsc -b --noEmit    # type-check
  PATH="/Users/admin/.nvm/versions/node/v22.18.0/bin:/usr/bin:/bin" ./node_modules/.bin/vite build         # production build
  PATH="/Users/admin/.nvm/versions/node/v22.18.0/bin:/usr/bin:/bin" ./node_modules/.bin/playwright test    # e2e tests
  ```
- **Package manager**: npm
- **Path aliases**: `@/` maps to `frontend/src/` (configured in tsconfig + vite)

## Testing

- Backend: `uv run pytest` (pytest with coverage plugin available)
- Frontend: TypeScript compilation (`tsc -b --noEmit`) + Vite build (see Node.js section for invocation)
- Always run relevant tests before closing a bead
