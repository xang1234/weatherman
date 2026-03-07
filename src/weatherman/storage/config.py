"""Storage configuration for S3-compatible backends."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class StorageConfig:
    """Configuration for the object storage backend.

    Supports any S3-compatible store (AWS S3, MinIO, etc.).
    """

    bucket: str
    endpoint_url: str | None = None  # None = real AWS S3
    region: str = "us-east-1"
    prefix: str = ""  # optional key prefix within the bucket

    def full_path(self, relative_path: str) -> str:
        """Join bucket prefix with a relative storage path.

        >>> cfg = StorageConfig(bucket="wx-data", prefix="prod")
        >>> cfg.full_path("models/gfs/catalog.json")
        'prod/models/gfs/catalog.json'
        """
        if self.prefix:
            return f"{self.prefix}/{relative_path}"
        return relative_path

    def s3_uri(self, relative_path: str) -> str:
        """Full S3 URI for a relative storage path.

        >>> cfg = StorageConfig(bucket="wx-data")
        >>> cfg.s3_uri("models/gfs/catalog.json")
        's3://wx-data/models/gfs/catalog.json'
        """
        return f"s3://{self.bucket}/{self.full_path(relative_path)}"
