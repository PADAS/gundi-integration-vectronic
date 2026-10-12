import pytest
import httpx
from unittest.mock import AsyncMock, patch, MagicMock
import logging
from datetime import datetime, timezone
from app.actions import client

AFTER = datetime(2024, 1, 1, tzinfo=timezone.utc)

@pytest.mark.asyncio
@patch("httpx.AsyncClient.get", new_callable=AsyncMock)
async def test_get_observations_success(mock_get):
    # Mock response with valid data
    mock_response = MagicMock()
    mock_response.is_error = False
    mock_response.json = MagicMock(return_value=[
        {
            "idCollar": 1,
            "acquisitionTime": "2024-01-01T00:00:00Z",
            "originCode": "A",
            "ecefX": 1, "ecefY": 2, "ecefZ": 3,
            "latitude": 10.0, "longitude": 20.0, "height": 100,
            "dop": 1.1, "mainVoltage": 3.7, "backupVoltage": 3.6, "temperature": 25.0
        }
    ])
    mock_response.text = "ok"
    mock_response.raise_for_status.return_value = None
    mock_get.return_value = mock_response

    result = await client.get_observations(base_url="http://test", collar_id=1, collar_key="key", after_scts=AFTER)
    assert isinstance(result, list)
    assert result[0].id_collar == 1

@pytest.mark.asyncio
@patch("httpx.AsyncClient.get", new_callable=AsyncMock)
async def test_get_observations_empty_response(mock_get):
    mock_response = MagicMock()
    mock_response.is_error = False
    mock_response.json = MagicMock(return_value=[])
    mock_response.text = ""
    mock_response.raise_for_status.return_value = None
    mock_get.return_value = mock_response

    result = await client.get_observations(base_url="http://test", collar_id=1, collar_key="key", after_scts=AFTER)
    assert result == []

@pytest.mark.asyncio
@patch("httpx.AsyncClient.get", new_callable=AsyncMock)
async def test_get_observations_403_forbidden(mock_get):
    mock_response = MagicMock()
    mock_response.is_error = True
    mock_response.text = "forbidden"
    error = httpx.HTTPStatusError("Forbidden", request=MagicMock(), response=MagicMock(status_code=403))
    mock_response.raise_for_status.side_effect = error
    mock_response.json = MagicMock()
    mock_get.return_value = mock_response

    with pytest.raises(client.VectronicForbiddenException):
        await client.get_observations(base_url="http://test", collar_id=1, collar_key="key", after_scts=AFTER)

@pytest.mark.asyncio
@patch("httpx.AsyncClient.get", new_callable=AsyncMock)
async def test_get_observations_404_not_found(mock_get):
    mock_response = MagicMock()
    mock_response.is_error = True
    mock_response.text = "not found"
    error = httpx.HTTPStatusError("Not Found", request=MagicMock(), response=MagicMock(status_code=404))
    mock_response.raise_for_status.side_effect = error
    mock_response.json = MagicMock()
    mock_get.return_value = mock_response

    with pytest.raises(client.VectronicNotFoundException):
        await client.get_observations(base_url="http://test", collar_id=1, collar_key="key", after_scts=AFTER)

@pytest.mark.asyncio
@patch("httpx.AsyncClient.get", new_callable=AsyncMock)
async def test_get_observations_other_http_error(mock_get):
    mock_response = MagicMock()
    mock_response.is_error = True
    mock_response.text = "bad request"
    error = httpx.HTTPStatusError("Bad Request", request=MagicMock(), response=MagicMock(status_code=400))
    mock_response.raise_for_status.side_effect = error
    mock_response.json = MagicMock()
    mock_get.return_value = mock_response

    with pytest.raises(httpx.HTTPStatusError):
        await client.get_observations(base_url="http://test", collar_id=1, collar_key="key", after_scts=AFTER)


def test_observation_parses_scts_as_utc():
    ob = client.VectronicObservation.parse_obj({
        "idCollar": 37590,
        "acquisitionTime": "2020-07-22T13:01:39",
        "scts": "2026-10-12T03:15:27",
    })
    assert ob.scts.isoformat() == "2026-10-12T03:15:27+00:00"


def test_observation_scts_is_optional():
    ob = client.VectronicObservation.parse_obj({"idCollar": 1, "acquisitionTime": "2024-01-01T00:00:00"})
    assert ob.scts is None


def test_observation_normalizes_offset_timestamps_to_utc():
    ob = client.VectronicObservation.parse_obj({
        "idCollar": 1,
        "acquisitionTime": "2020-07-22T13:01:39+02:00",
        "scts": "2026-10-12T03:15:27+02:00",
    })
    assert ob.acquisition_time.isoformat() == "2020-07-22T11:01:39+00:00"
    assert ob.scts.isoformat() == "2026-10-12T01:15:27+00:00"


def test_httpx_request_log_redacts_collar_key(caplog):
    with caplog.at_level(logging.INFO, logger="httpx"):
        logging.getLogger("httpx").info(
            'HTTP Request: %s %s "%s %d %s"',
            "GET",
            "https://api.vectronic-wildlife.com/v2/collar/1/gps?collarkey=SECRETKEY123&afterScts=2024-01-01T00%3A00%3A00",
            "HTTP/1.1", 200, "OK",
        )
    assert "SECRETKEY123" not in caplog.text
    assert "collarkey=<redacted>&afterScts=" in caplog.text


@pytest.mark.asyncio
@patch("httpx.AsyncClient.get", new_callable=AsyncMock)
async def test_get_observations_sends_key_and_after_scts(mock_get):
    mock_response = MagicMock(is_error=False, text="")
    mock_response.json = MagicMock(return_value=[])
    mock_get.return_value = mock_response
    await client.get_observations(base_url="http://test", collar_id=7, collar_key="key", after_scts=AFTER)
    args, kwargs = mock_get.call_args
    assert args[0] == "http://test/v2/collar/7/gps"
    assert kwargs["params"] == {"collarkey": "key", "afterScts": "2024-01-01T00:00:00"}


@pytest.mark.asyncio
@pytest.mark.parametrize("status_code,expected", [
    (403, client.VectronicForbiddenException),
    (404, client.VectronicNotFoundException),
    (500, httpx.HTTPStatusError),
])
@patch("httpx.AsyncClient.get", new_callable=AsyncMock)
async def test_get_observations_http_errors_do_not_expose_collar_key(mock_get, status_code, expected):
    # httpx's real message embeds the request URL, query string included.
    url = "https://api.vectronic-wildlife.com/v2/collar/1/gps?collarkey=SECRETKEY123&afterScts=2024-01-01T00%3A00%3A00"
    request = httpx.Request("GET", url)
    response = httpx.Response(status_code=status_code, request=request)
    mock_response = MagicMock(is_error=True, text="error")
    mock_response.raise_for_status.side_effect = httpx.HTTPStatusError(
        f"Client error '{status_code}' for url '{url}'", request=request, response=response
    )
    mock_get.return_value = mock_response

    with pytest.raises(expected) as exc_info:
        await client.get_observations(base_url="http://test", collar_id=1, collar_key="SECRETKEY123", after_scts=AFTER)

    assert "SECRETKEY123" not in str(exc_info.value)
    assert exc_info.value.__cause__ is None
    assert exc_info.value.__suppress_context__
