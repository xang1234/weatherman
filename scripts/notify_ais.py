"""Emit an ais.refreshed SSE event to tell the frontend which AIS date to display.

Usage (while the server is running in another terminal):
    uv run python scripts/notify_ais.py 2025-12-25
"""

import sys
from datetime import date

from weatherman.events.emissions import emit_ais_refreshed


def main() -> None:
    if len(sys.argv) < 2:
        print(f"Usage: {sys.argv[0]} <YYYY-MM-DD>")
        sys.exit(1)

    ais_date = date.fromisoformat(sys.argv[1])
    emit_ais_refreshed(
        ais_date=ais_date,
        tile_url_template=f"/ais/tiles/{ais_date}/{{z}}/{{x}}/{{y}}.pbf",
    )
    print(f"Emitted ais.refreshed for {ais_date}")


if __name__ == "__main__":
    main()
