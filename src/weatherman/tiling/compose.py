"""Docker Compose file generation for TiTiler deployment.

Generates a docker-compose.yml service definition from TiTilerConfig.
This keeps the deployment config in sync with the Python configuration
rather than maintaining a separate static YAML file.
"""

from __future__ import annotations

import yaml

from weatherman.tiling.config import TiTilerConfig


def generate_compose_dict(config: TiTilerConfig) -> dict:
    """Build a docker-compose service dict for TiTiler.

    Returns a dict suitable for yaml.dump() that defines a single
    TiTiler service with all environment variables, port mapping,
    memory limits, and health check.
    """
    env = config.env_vars()
    memory_mb = config.memory_limit_mb()

    service: dict = {
        "image": config.image,
        "container_name": config.container_name,
        "ports": [f"{config.port}:{config.port}"],
        "environment": env,
        "deploy": {
            "resources": {
                "limits": {"memory": f"{memory_mb}M"},
                "reservations": {"memory": f"{memory_mb // 2}M"},
            }
        },
        "healthcheck": {
            "test": ["CMD", "curl", "-f", f"http://localhost:{config.port}/api"],
            "interval": "30s",
            "timeout": "10s",
            "retries": 3,
            "start_period": "10s",
        },
        "restart": "unless-stopped",
    }

    return {
        "version": "3.8",
        "services": {"titiler": service},
    }


def render_compose_yaml(config: TiTilerConfig) -> str:
    """Render a complete docker-compose YAML string for TiTiler."""
    data = generate_compose_dict(config)
    return yaml.dump(data, default_flow_style=False, sort_keys=False)
