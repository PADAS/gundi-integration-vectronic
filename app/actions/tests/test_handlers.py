import pytest
import json
import httpx
from app import settings
from pydantic import ValidationError
from gundi_core.schemas.v2 import LogLevel
from unittest.mock import AsyncMock, MagicMock
from app.actions.client import (
    VectronicObservation,
    VectronicForbiddenException,
    VectronicNotFoundException,
)
from app.actions.handlers import (
    action_pull_observations,
    action_fetch_collar_observations,
    CollarData,
    transform,
)
from app.actions.configurations import PullObservationsConfig, PullCollarObservationsConfig

@pytest.mark.asyncio
async def test_action_pull_observations_triggers_fetch_collar_observations(mocker, mock_publish_event, mock_state_manager):
    integration = MagicMock(id=1)
    files = json.dumps([
        {"parsedData": {"collarID": "1", "collarType": "A", "comID": "X", "comType": "Y", "key": "K"}}
    ])
    config = PullObservationsConfig(files=files, default_lookback_hours=12)
    mock_state_manager.get_state = AsyncMock(return_value=None)
    settings.TRIGGER_ACTIONS_ALWAYS_SYNC = False
    settings.INTEGRATION_COMMANDS_TOPIC = "vectronic-actions-topic"

    mocker.patch("app.services.state.IntegrationStateManager.get_state", return_value=None)
    mocker.patch("app.services.activity_logger.publish_event", mock_publish_event)
    mocker.patch("app.services.action_runner.publish_event", mock_publish_event)

    mock_trigger_action = mocker.patch("app.actions.handlers.trigger_action", return_value=None)

    mocker.patch("app.services.action_scheduler.publish_event", mock_publish_event)
    mocker.patch("app.services.action_runner.execute_action", return_value=None)
    result = await action_pull_observations(integration, config)
    assert result["status"] == "success"
    assert result["collars_triggered"] == 1
    mock_trigger_action.assert_called_once()

@pytest.mark.asyncio
async def test_action_pull_observations_bad_json(mocker, mock_publish_event, mock_state_manager):
    mocker.patch("app.services.activity_logger.publish_event", mock_publish_event)
    mocker.patch("app.services.action_runner.publish_event", mock_publish_event)
    mocker.patch("app.services.action_scheduler.publish_event", mock_publish_event)
    integration = MagicMock(id=1)
    config = PullObservationsConfig(files="not a json", default_lookback_hours=12)
    with pytest.raises(json.JSONDecodeError):
        await action_pull_observations(integration, config)

@pytest.mark.asyncio
async def test_action_pull_observations_empty_list(mocker, mock_publish_event, mock_state_manager):
    mocker.patch("app.services.activity_logger.publish_event", mock_publish_event)
    mocker.patch("app.services.action_runner.publish_event", mock_publish_event)
    mocker.patch("app.services.action_scheduler.publish_event", mock_publish_event)
    integration = MagicMock(id=1)
    config = PullObservationsConfig(files="[]", default_lookback_hours=12)
    result = await action_pull_observations(integration, config)
    assert result["status"] == "success"
    assert result["collars_triggered"] == 0

@pytest.mark.asyncio
async def test_action_fetch_collar_observations_no_observations(mocker):
    mock_get_obs = mocker.patch("app.actions.handlers.client.get_observations", new_callable=AsyncMock)
    mocker.patch("app.services.activity_logger.publish_event", new=AsyncMock())
    mocker.patch("app.services.action_runner.publish_event", new=AsyncMock())
    mocker.patch("app.services.action_scheduler.publish_event", new=AsyncMock())
    integration = MagicMock(id=1, base_url=None)
    config = PullCollarObservationsConfig(start="2024-01-01T00:00:00", collar_id=1, collar_key="K")
    mock_get_obs.return_value = []
    result = await action_fetch_collar_observations(integration, config)
    assert result["observations_extracted"] == 0

@pytest.mark.asyncio
async def test_action_fetch_collar_observations_skips_invalid_location(mocker, caplog):
    # Mock integration and action_config
    integration = MagicMock()
    integration.id = 1
    integration.base_url = None

    action_config = MagicMock()
    action_config.collar_id = 123

    # Mock observation with invalid latitude/longitude
    invalid_ob = MagicMock()
    invalid_ob.latitude = None
    invalid_ob.longitude = 10.0
    invalid_ob.id_collar = "test_collar"
    invalid_ob.acquisition_time = "2024-01-01T00:00:00Z"
    invalid_ob.dict.return_value = {"latitude": None, "longitude": 10.0}

    mocker.patch("app.services.activity_logger.publish_event", new=AsyncMock())
    mocker.patch("app.services.action_runner.publish_event", new=AsyncMock())
    mocker.patch("app.services.action_scheduler.publish_event", new=AsyncMock())

    mock_log_action_activity = mocker.patch("app.actions.handlers.log_action_activity", new_callable=AsyncMock)
    mocker.patch("app.actions.handlers.client.get_observations", new=AsyncMock(return_value=[invalid_ob]))

    result = await action_fetch_collar_observations(integration, action_config)

    # Assert warning was logged and no observations extracted
    assert "invalid observation" in caplog.text
    assert result["observations_extracted"] == 0

    mock_log_action_activity.assert_awaited_once()
    assert mock_log_action_activity.call_args[1]["integration_id"] == integration.id
    assert mock_log_action_activity.call_args[1]["level"] == LogLevel.WARNING
    assert mock_log_action_activity.call_args[1]["title"] == f"Collar ID {invalid_ob.id_collar} got an invalid observation (location is invalid). Skipping..."

@pytest.mark.asyncio
async def test_action_fetch_collar_observations_exception_sends_error_activity_log(mocker):
    mock_get_obs = mocker.patch("app.actions.handlers.client.get_observations", new_callable=AsyncMock)
    mock_log_action_activity = mocker.patch("app.actions.handlers.log_action_activity", new_callable=AsyncMock)
    mocker.patch("app.services.activity_logger.publish_event", new=AsyncMock())
    integration = MagicMock(id=1, base_url=None)
    config = PullCollarObservationsConfig(start="2024-01-01T00:00:00", collar_id=1, collar_key="K")
    mock_get_obs.side_effect = Exception("fail")
    result = await action_fetch_collar_observations(integration, config)
    assert result == {"observations_extracted": 0}
    mock_log_action_activity.assert_awaited_once()
    assert mock_log_action_activity.call_args[1]["integration_id"] == integration.id
    assert mock_log_action_activity.call_args[1]["action_id"] == "fetch_collar_observations"
    assert mock_log_action_activity.call_args[1]["level"] == LogLevel.ERROR
    assert mock_log_action_activity.call_args[1]["title"] == f"Failed to fetch observations for collar {config.collar_id}."

@pytest.mark.asyncio
async def test_action_fetch_collar_observations_forbidden_logs_warning(mocker):
    mock_get_obs = mocker.patch("app.actions.handlers.client.get_observations", new_callable=AsyncMock)
    mock_log_action_activity = mocker.patch("app.actions.handlers.log_action_activity", new_callable=AsyncMock)
    mocker.patch("app.services.activity_logger.publish_event", new=AsyncMock())
    integration = MagicMock(id=1, base_url=None)
    config = PullCollarObservationsConfig(start="2024-01-01T00:00:00", collar_id=1, collar_key="K")
    mock_get_obs.side_effect = VectronicForbiddenException(Exception("403"), "Unauthorized access")
    result = await action_fetch_collar_observations(integration, config)
    assert result == {"observations_extracted": 0}
    mock_log_action_activity.assert_awaited_once()
    assert mock_log_action_activity.call_args[1]["action_id"] == "fetch_collar_observations"
    assert mock_log_action_activity.call_args[1]["level"] == LogLevel.WARNING

@pytest.mark.asyncio
async def test_action_fetch_collar_observations_not_found_logs_warning(mocker):
    mock_get_obs = mocker.patch("app.actions.handlers.client.get_observations", new_callable=AsyncMock)
    mock_log_action_activity = mocker.patch("app.actions.handlers.log_action_activity", new_callable=AsyncMock)
    mocker.patch("app.services.activity_logger.publish_event", new=AsyncMock())
    integration = MagicMock(id=1, base_url=None)
    config = PullCollarObservationsConfig(start="2024-01-01T00:00:00", collar_id=1, collar_key="K")
    mock_get_obs.side_effect = VectronicNotFoundException(Exception("404"), "Not found")
    result = await action_fetch_collar_observations(integration, config)
    assert result == {"observations_extracted": 0}
    mock_log_action_activity.assert_awaited_once()
    assert mock_log_action_activity.call_args[1]["action_id"] == "fetch_collar_observations"
    assert mock_log_action_activity.call_args[1]["level"] == LogLevel.WARNING

@pytest.mark.asyncio
async def test_action_fetch_collar_observations_http_status_error_logs_warning(mocker):
    mock_get_obs = mocker.patch("app.actions.handlers.client.get_observations", new_callable=AsyncMock)
    mock_log_action_activity = mocker.patch("app.actions.handlers.log_action_activity", new_callable=AsyncMock)
    mocker.patch("app.services.activity_logger.publish_event", new=AsyncMock())
    integration = MagicMock(id=1, base_url=None)
    config = PullCollarObservationsConfig(start="2024-01-01T00:00:00", collar_id=1, collar_key="K")
    request = httpx.Request("GET", "https://api.vectronic-wildlife.com/v2/collar/1/gps")
    response = httpx.Response(status_code=500, request=request, text="Internal Server Error")
    mock_get_obs.side_effect = httpx.HTTPStatusError("Server error", request=request, response=response)
    result = await action_fetch_collar_observations(integration, config)
    assert result == {"observations_extracted": 0}
    mock_log_action_activity.assert_awaited_once()
    assert mock_log_action_activity.call_args[1]["action_id"] == "fetch_collar_observations"
    assert mock_log_action_activity.call_args[1]["level"] == LogLevel.WARNING

def test_transform_ok():
    obj = {
        "idCollar": "1",
        "acquisitionTime": "2024-01-01T00:00:00",
        "originCode": "A",
        "ecefX": 100,
        "ecefY": 200,
        "ecefZ": 300,
        "latitude": 1.0,
        "longitude": 2.0,
        "height": 10,
        "dop": 1.5,
        "mainVoltage": 3.7,
        "backupVoltage": 3.6,
        "temperature": 25.0,
        "foo": "bar"  # extra field, will be ignored
    }
    obs = VectronicObservation.parse_obj(obj)
    result = transform(obs)
    assert result["source_name"] == 1
    assert result["location"]["lat"] == 1.0
    assert result["location"]["lon"] == 2.0

def test_collar_data_ok():
    data = {"collarID": "1", "collarType": "A", "comID": "X", "comType": "Y", "key": "K"}
    model = CollarData.parse_obj(data)
    assert model.collar_id == "1"

def test_collar_data_parsing_error():
    data = {"collarType": "A", "comID": "X", "comType": "Y", "key": "K"}
    with pytest.raises(ValidationError) as exc_info:
        CollarData.parse_obj(data)
    assert "collarID" in str(exc_info.value)
