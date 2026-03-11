FROM ghcr.io/astral-sh/uv:python3.14-bookworm-slim

# rasterio/GDAL + DuckDB native deps
RUN apt-get update && apt-get install -y --no-install-recommends \
    libgdal-dev g++ curl && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-dev
COPY src/ src/
COPY scripts/ scripts/

EXPOSE 8000
CMD ["uv", "run", "python", "-m", "weatherman"]
