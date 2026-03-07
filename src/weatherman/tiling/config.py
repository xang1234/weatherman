"""TiTiler deployment configuration.

Generates environment variables and Docker configuration for TiTiler
to serve COGs from our S3-compatible object storage.
"""

from __future__ import annotations

from dataclasses import dataclass, field

from weatherman.storage.config import StorageConfig

# Default tuning parameters
DEFAULT_WORKERS = 4
DEFAULT_WORKER_MEMORY_MB = 512
DEFAULT_PORT = 8080
DEFAULT_TIMEOUT = 60


@dataclass(frozen=True)
class TiTilerConfig:
    """Configuration for TiTiler tile server deployment.

    Generates environment variables for the developmentseed/titiler
    Docker image, including S3 access, CORS, worker tuning, and
    GDAL performance settings.
    """

    storage: StorageConfig
    workers: int = DEFAULT_WORKERS
    port: int = DEFAULT_PORT
    worker_timeout: int = DEFAULT_TIMEOUT
    cors_origins: list[str] = field(default_factory=lambda: ["*"])
    image: str = "ghcr.io/developmentseed/titiler:0.18.5"
    container_name: str = "weatherman-titiler"
    gdal_cachemax_mb: int = 200
    gdal_band_block_cache: str = "HASHSET"
    gdal_disable_readdir: bool = True
    gdal_http_merge_consecutive_ranges: bool = True
    gdal_http_multiplex: bool = True
    gdal_http_version: int = 2
    cpl_vsil_curl_cache_size_mb: int = 200

    def env_vars(self) -> dict[str, str]:
        """Build environment variable dict for the TiTiler container."""
        env: dict[str, str] = {}

        # S3 credentials — use variable substitution placeholders so that
        # generated Compose files never contain literal secrets.
        env["AWS_ACCESS_KEY_ID"] = "${AWS_ACCESS_KEY_ID}"
        env["AWS_SECRET_ACCESS_KEY"] = "${AWS_SECRET_ACCESS_KEY}"

        # S3 access configuration
        if self.storage.endpoint_url:
            env["AWS_S3_ENDPOINT"] = self.storage.endpoint_url
            # For non-AWS S3 (MinIO etc.), force path-style access
            env["AWS_VIRTUAL_HOSTING"] = "FALSE"
        if self.storage.region:
            env["AWS_DEFAULT_REGION"] = self.storage.region

        # CORS
        env["TITILER_API_CORS_ORIGINS"] = ",".join(self.cors_origins)

        # Uvicorn / Gunicorn workers
        env["WEB_CONCURRENCY"] = str(self.workers)
        env["WORKERS_PER_CORE"] = "1"
        env["GRACEFUL_TIMEOUT"] = str(self.worker_timeout)
        env["TIMEOUT"] = str(self.worker_timeout)

        # GDAL performance tuning for cloud-hosted COGs
        env["GDAL_CACHEMAX"] = str(self.gdal_cachemax_mb)
        env["GDAL_BAND_BLOCK_CACHE"] = self.gdal_band_block_cache
        env["GDAL_HTTP_MERGE_CONSECUTIVE_RANGES"] = (
            "YES" if self.gdal_http_merge_consecutive_ranges else "NO"
        )
        env["GDAL_HTTP_MULTIPLEX"] = "YES" if self.gdal_http_multiplex else "NO"
        env["GDAL_HTTP_VERSION"] = str(self.gdal_http_version)
        env["CPL_VSIL_CURL_CACHE_SIZE"] = str(
            self.cpl_vsil_curl_cache_size_mb * 1024 * 1024
        )
        env["VSI_CACHE"] = "TRUE"
        env["VSI_CACHE_SIZE"] = str(self.cpl_vsil_curl_cache_size_mb * 1024 * 1024)
        if self.gdal_disable_readdir:
            env["GDAL_DISABLE_READDIR_ON_OPEN"] = "EMPTY_DIR"

        return env

    def memory_limit_mb(self) -> int:
        """Total container memory limit based on worker count."""
        # Workers + overhead for the main process
        return self.workers * DEFAULT_WORKER_MEMORY_MB + 256

    @classmethod
    def from_storage(
        cls,
        storage: StorageConfig,
        *,
        workers: int = DEFAULT_WORKERS,
        cors_origins: list[str] | None = None,
    ) -> TiTilerConfig:
        """Create a TiTiler config from an existing StorageConfig."""
        kwargs: dict = {"storage": storage, "workers": workers}
        if cors_origins is not None:
            kwargs["cors_origins"] = cors_origins
        return cls(**kwargs)
