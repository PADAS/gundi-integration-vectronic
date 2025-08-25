import pytest
import httpx
from unittest.mock import AsyncMock, patch, MagicMock
from app.actions import client

@pytest.mark.asyncio
@patch("httpx.AsyncClient.get", new_callable=AsyncMock)
async def test_get_observations_success(mock_get):
    # Mock response with valid data
    mock_response = MagicMock()
    mock_response.is_error = False
    mock_response.json.return_value = [
        {
            "idCollar": 1,
            "acquisitionTime": "2024-01-01T00:00:00Z",
            "originCode": "A",
            "ecefX": 1, "ecefY": 2, "ecefZ": 3,
            "latitude": 10.0, "longitude": 20.0, "height": 100,
            "dop": 1.1, "mainVoltage": 3.7, "backupVoltage": 3.6, "temperature": 25.0
        }
    ]
    mock_response.text = "ok"
    mock_response.raise_for_status.return_value = None
    mock_get.return_value = mock_response

    integration = MagicMock(id=1)
    config = MagicMock(collar_id=1, collar_key="key", start=MagicMock(isoformat=lambda: "2024-01-01T00:00:00+00:00"))
    result = await client.get_observations(integration, "http://test", config)
    assert isinstance(result, list)
    assert result[0].idCollar == 1

@pytest.mark.asyncio
@patch("httpx.AsyncClient.get", new_callable=AsyncMock)
async def test_get_observations_empty_response(mock_get):
    mock_response = MagicMock()
    mock_response.is_error = False
    mock_response.json.return_value = []
    mock_response.text = ""
    mock_response.raise_for_status.return_value = None
    mock_get.return_value = mock_response

    integration = MagicMock(id=1)
    config = MagicMock(collar_id=1, collar_key="key", start=MagicMock(isoformat=lambda: "2024-01-01T00:00:00+00:00"))
    result = await client.get_observations(integration, "http://test", config)
    assert result == ""

@pytest.mark.asyncio
@patch("httpx.AsyncClient.get", new_callable=AsyncMock)
async def test_get_observations_403_forbidden(mock_get):
    mock_response = MagicMock()
    mock_response.is_error = True
    mock_response.text = "forbidden"
    error = httpx.HTTPStatusError("Forbidden", request=MagicMock(), response=MagicMock(status_code=403))
    mock_response.raise_for_status.side_effect = error
    mock_get.return_value = mock_response

    integration = MagicMock(id=1)
    config = MagicMock(collar_id=1, collar_key="key", start=MagicMock(isoformat=lambda: "2024-01-01T00:00:00+00:00"))
    with pytest.raises(client.VectronicForbiddenException):
        await client.get_observations(integration, "http://test", config)

@pytest.mark.asyncio
@patch("httpx.AsyncClient.get", new_callable=AsyncMock)
async def test_get_observations_404_not_found(mock_get):
    mock_response = MagicMock()
    mock_response.is_error = True
    mock_response.text = "not found"
    error = httpx.HTTPStatusError("Not Found", request=MagicMock(), response=MagicMock(status_code=404))
    mock_response.raise_for_status.side_effect = error
    mock_get.return_value = mock_response

    integration = MagicMock(id=1)
    config = MagicMock(collar_id=1, collar_key="key", start=MagicMock(isoformat=lambda: "2024-01-01T00:00:00+00:00"))
    with pytest.raises(client.VectronicNotFoundException):
        await client.get_observations(integration, "http://test", config)

@pytest.mark.asyncio
@patch("httpx.AsyncClient.get", new_callable=AsyncMock)
async def test_get_observations_other_http_error(mock_get):
    mock_response = MagicMock()
    mock_response.is_error = True
    mock_response.text = "bad request"
    error = httpx.HTTPStatusError("Bad Request", request=MagicMock(), response=MagicMock(status_code=400))
    mock_response.raise_for_status.side_effect = error
    mock_get.return_value = mock_response

    integration = MagicMock(id=1)
    config = MagicMock(collar_id=1, collar_key="key", start=MagicMock(isoformat=lambda: "2024-01-01T00:00:00+00:00"))
    with pytest.raises(httpx.HTTPStatusError):
        await client.get_observations(integration, "http://test", config)
