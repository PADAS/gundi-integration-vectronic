import pytest
import json
from app import settings
from pydantic import ValidationError
from unittest.mock import AsyncMock, patch, MagicMock
from app.actions.client import VectronicObservation
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
@patch("app.actions.handlers.client.get_observations", new_callable=AsyncMock)
async def test_action_fetch_collar_observations_no_observations(mock_get_obs, mocker):
    mocker.patch("app.services.activity_logger.publish_event", new=AsyncMock())
    mocker.patch("app.services.action_runner.publish_event", new=AsyncMock())
    mocker.patch("app.services.action_scheduler.publish_event", new=AsyncMock())
    integration = MagicMock(id=1, base_url=None)
    config = PullCollarObservationsConfig(start="2024-01-01T00:00:00", collar_id=1, collar_key="K")
    mock_get_obs.return_value = []
    result = await action_fetch_collar_observations(integration, config)
    assert result["observations_extracted"] == 0

@pytest.mark.asyncio
@patch("app.actions.handlers.client.get_observations", new_callable=AsyncMock)
async def test_action_fetch_collar_observations_exception(mock_get_obs):
    integration = MagicMock(id=1, base_url=None)
    config = PullCollarObservationsConfig(start="2024-01-01T00:00:00", collar_id=1, collar_key="K")
    mock_get_obs.side_effect = Exception("fail")
    with pytest.raises(Exception):
        await action_fetch_collar_observations(integration, config)

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
