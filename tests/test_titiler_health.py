"""Tests for TiTilerHealthCheck (wx-wrq).

Verifies the health check conforms to the DependencyChecker protocol
and correctly reports TiTiler reachability.
"""

from __future__ import annotations

import inspect
from unittest.mock import AsyncMock, patch

import httpx
import pytest

from weatherman.app import TiTilerHealthCheck


class TestTiTilerHealthCheck:
    def test_conforms_to_protocol(self):
        check = TiTilerHealthCheck("http://localhost:8080")
        assert check.name == "titiler"
        assert check.critical is True
        assert inspect.iscoroutinefunction(check.check)

    def test_strips_trailing_slash(self):
        check = TiTilerHealthCheck("http://localhost:8080/")
        assert check._url == "http://localhost:8080/api"

    @pytest.mark.anyio
    async def test_returns_true_on_200(self):
        check = TiTilerHealthCheck("http://localhost:8080")
        mock_resp = AsyncMock()
        mock_resp.status_code = 200

        with patch("weatherman.app.httpx.AsyncClient") as mock_client_cls:
            mock_client = AsyncMock()
            mock_client.get.return_value = mock_resp
            mock_client_cls.return_value.__aenter__ = AsyncMock(return_value=mock_client)
            mock_client_cls.return_value.__aexit__ = AsyncMock(return_value=False)

            result = await check.check()
            assert result is True
            mock_client.get.assert_called_once_with("http://localhost:8080/api")

    @pytest.mark.anyio
    async def test_returns_false_on_non_200(self):
        check = TiTilerHealthCheck("http://localhost:8080")
        mock_resp = AsyncMock()
        mock_resp.status_code = 503

        with patch("weatherman.app.httpx.AsyncClient") as mock_client_cls:
            mock_client = AsyncMock()
            mock_client.get.return_value = mock_resp
            mock_client_cls.return_value.__aenter__ = AsyncMock(return_value=mock_client)
            mock_client_cls.return_value.__aexit__ = AsyncMock(return_value=False)

            result = await check.check()
            assert result is False

    @pytest.mark.anyio
    async def test_returns_false_on_connection_error(self):
        check = TiTilerHealthCheck("http://localhost:8080")

        with patch("weatherman.app.httpx.AsyncClient") as mock_client_cls:
            mock_client = AsyncMock()
            mock_client.get.side_effect = httpx.ConnectError("refused")
            mock_client_cls.return_value.__aenter__ = AsyncMock(return_value=mock_client)
            mock_client_cls.return_value.__aexit__ = AsyncMock(return_value=False)

            result = await check.check()
            assert result is False

    @pytest.mark.anyio
    async def test_returns_false_on_timeout(self):
        check = TiTilerHealthCheck("http://localhost:8080")

        with patch("weatherman.app.httpx.AsyncClient") as mock_client_cls:
            mock_client = AsyncMock()
            mock_client.get.side_effect = httpx.TimeoutException("timed out")
            mock_client_cls.return_value.__aenter__ = AsyncMock(return_value=mock_client)
            mock_client_cls.return_value.__aexit__ = AsyncMock(return_value=False)

            result = await check.check()
            assert result is False

    def test_registered_in_readiness_probe(self, tmp_path):
        """Verify the health check is registered during app lifespan."""
        from starlette.testclient import TestClient
        from weatherman.app import create_app
        from weatherman.health import _checkers
        from weatherman.storage.paths import StorageLayout

        # Need minimal data_dir with a catalog so app can start
        model = "gfs"
        layout = StorageLayout(model)
        catalog_dir = tmp_path / layout.catalog_path
        catalog_dir.parent.mkdir(parents=True, exist_ok=True)
        catalog_dir.write_text('{"schema_version":1,"model":"gfs","current_run_id":null,"runs":[]}')

        app = create_app(data_dir=str(tmp_path), titiler_base_url="http://localhost:9999")
        with TestClient(app) as client:
            # During lifespan, TiTilerHealthCheck should be registered
            titiler_checks = [c for c in _checkers if c.name == "titiler"]
            assert len(titiler_checks) == 1
            assert titiler_checks[0].critical is True

            # /health/ready should include titiler in checks (it will fail since no server)
            resp = client.get("/health/ready")
            body = resp.json()
            assert "titiler" in body["checks"]
            assert body["checks"]["titiler"]["status"] == "fail"
            assert body["status"] == "not_ready"
