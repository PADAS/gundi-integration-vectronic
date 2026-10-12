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
from app.conftest import async_return

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

    mock_trigger_actions = mocker.patch("app.actions.handlers.trigger_actions", return_value=None)

    mocker.patch("app.services.action_scheduler.publish_event", mock_publish_event)
    mocker.patch("app.services.action_runner.execute_action", return_value=None)
    result = await action_pull_observations(integration, config)
    assert result["status"] == "success"
    assert result["collars_triggered"] == 1
    # One batched publish for every collar, not one PubSub round trip per
    # collar: the 133-collar integrations overran Cloud Run's request timeout.
    mock_trigger_actions.assert_called_once()
    args, kwargs = mock_trigger_actions.call_args
    assert kwargs["action_id"] == "fetch_collar_observations"
    configs = kwargs["configs"]
    assert [c.collar_id for c in configs] == [1]
    assert configs[0].collar_key == "K"

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

def make_observation(acquisition_time, latitude=1.0, longitude=2.0, collar_id=123):
    return VectronicObservation(
        id_collar=collar_id,
        acquisition_time=acquisition_time,
        latitude=latitude,
        longitude=longitude,
    )


@pytest.mark.asyncio
async def test_action_fetch_collar_observations_summarizes_invalid_locations(mocker, caplog, mock_state_manager):
    mocker.patch("app.services.activity_logger.publish_event", new=AsyncMock())
    integration = MagicMock(id=1, base_url=None)
    config = PullCollarObservationsConfig(start="2024-01-01T00:00:00", collar_id=123, collar_key="K")
    invalid_obs = [
        make_observation(f"2024-01-01T0{i}:00:00", latitude=None, longitude=None)
        for i in range(5)
    ]
    mocker.patch("app.actions.handlers.state_manager", mock_state_manager)
    mock_send = mocker.patch("app.actions.handlers.send_observations_to_gundi", new_callable=AsyncMock)
    mock_log_action_activity = mocker.patch("app.actions.handlers.log_action_activity", new_callable=AsyncMock)
    mocker.patch("app.actions.handlers.client.get_observations", new=AsyncMock(return_value=invalid_obs))

    result = await action_fetch_collar_observations(integration, config)

    assert result["observations_extracted"] == 0
    # Every skip still reaches the application log...
    assert caplog.text.count("invalid observation") == 5
    # ...but the activity log gets a single summary for the run.
    mock_log_action_activity.assert_awaited_once()
    kwargs = mock_log_action_activity.call_args[1]
    assert kwargs["integration_id"] == integration.id
    assert kwargs["level"] == LogLevel.WARNING
    assert kwargs["title"] == "Collar ID 123: skipped 5 observation(s) with an invalid location."
    assert kwargs["data"]["skipped_count"] == 5
    assert len(kwargs["data"]["sample_observations"]) == 3
    mock_send.assert_not_awaited()


@pytest.mark.asyncio
async def test_action_fetch_collar_observations_advances_cursor_when_all_invalid(mocker, mock_state_manager):
    mocker.patch("app.services.activity_logger.publish_event", new=AsyncMock())
    # Without advancing the cursor, the next run re-fetches and re-reports the same fixes.
    integration = MagicMock(id=1, base_url=None)
    config = PullCollarObservationsConfig(start="2022-09-01T00:00:00", collar_id=123, collar_key="K")
    invalid_obs = [
        make_observation("2022-09-26T16:01:39", latitude=None, longitude=None),
        make_observation("2022-09-25T10:00:00", latitude=None, longitude=None),
    ]
    mocker.patch("app.actions.handlers.state_manager", mock_state_manager)
    mocker.patch("app.actions.handlers.log_action_activity", new_callable=AsyncMock)
    mocker.patch("app.actions.handlers.client.get_observations", new=AsyncMock(return_value=invalid_obs))

    await action_fetch_collar_observations(integration, config)

    mock_state_manager.set_state.assert_called_once_with(
        integration_id=integration.id,
        action_id="pull_observations",
        state={"updated_at": "2022-09-26T16:01:39"},
        source_id="123",
    )


@pytest.mark.asyncio
async def test_action_fetch_collar_observations_cursor_never_moves_backward(mocker, mock_state_manager):
    # A late-uploaded fix passes the afterScts filter but has an acquisition
    # time older than the current cursor; keeping it would widen the next query.
    mocker.patch("app.services.activity_logger.publish_event", new=AsyncMock())
    integration = MagicMock(id=1, base_url=None)
    config = PullCollarObservationsConfig(start="2026-10-12T00:00:00", collar_id=123, collar_key="K")
    late_ob = make_observation("2022-09-26T16:01:39", latitude=None, longitude=None)
    mocker.patch("app.actions.handlers.state_manager", mock_state_manager)
    mocker.patch("app.actions.handlers.log_action_activity", new_callable=AsyncMock)
    mocker.patch("app.actions.handlers.client.get_observations", new=AsyncMock(return_value=[late_ob]))

    await action_fetch_collar_observations(integration, config)

    assert mock_state_manager.set_state.call_args[1]["state"] == {"updated_at": "2026-10-12T00:00:00"}


@pytest.mark.asyncio
async def test_action_fetch_collar_observations_mixed_valid_and_invalid(mocker, mock_state_manager):
    mocker.patch("app.services.activity_logger.publish_event", new=AsyncMock())
    integration = MagicMock(id=1, base_url=None)
    config = PullCollarObservationsConfig(start="2024-01-01T00:00:00", collar_id=123, collar_key="K")
    valid_ob = make_observation("2024-01-01T01:00:00")
    # The latest observation is invalid; the cursor still moves past it.
    invalid_ob = make_observation("2024-01-01T02:00:00", latitude=None)
    mocker.patch("app.actions.handlers.state_manager", mock_state_manager)
    mock_send = mocker.patch(
        "app.actions.handlers.send_observations_to_gundi", new=AsyncMock(return_value=[{"object_id": "x"}])
    )
    mock_log_action_activity = mocker.patch("app.actions.handlers.log_action_activity", new_callable=AsyncMock)
    mocker.patch("app.actions.handlers.client.get_observations", new=AsyncMock(return_value=[valid_ob, invalid_ob]))

    result = await action_fetch_collar_observations(integration, config)

    assert result == {"observations_extracted": 1}
    sent = mock_send.call_args[1]["observations"]
    assert len(sent) == 1
    assert sent[0]["location"] == {"lat": 1.0, "lon": 2.0}
    assert mock_log_action_activity.call_args[1]["data"]["skipped_count"] == 1
    assert mock_state_manager.set_state.call_args[1]["state"] == {"updated_at": "2024-01-01T02:00:00"}


@pytest.mark.asyncio
async def test_action_fetch_collar_observations_does_not_publish_started_event(mocker, mock_state_manager):
    mock_publish_event = mocker.patch("app.services.activity_logger.publish_event", new_callable=AsyncMock)
    mocker.patch("app.actions.handlers.client.get_observations", new=AsyncMock(return_value=[]))
    integration = MagicMock(id=1, base_url=None)
    config = PullCollarObservationsConfig(start="2024-01-01T00:00:00", collar_id=1, collar_key="K")

    await action_fetch_collar_observations(integration=integration, action_config=config)

    published = [c[1]["event"].event_type for c in mock_publish_event.call_args_list]
    assert published == ["IntegrationActionComplete"]


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
async def test_action_fetch_collar_observations_forbidden_logs_warning(mocker, mock_state_manager):
    mocker.patch("app.actions.handlers.state_manager", mock_state_manager)
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
async def test_action_fetch_collar_observations_not_found_logs_warning(mocker, mock_state_manager):
    mocker.patch("app.actions.handlers.state_manager", mock_state_manager)
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
async def test_action_fetch_collar_observations_http_status_error_logs_warning(mocker, mock_state_manager):
    mocker.patch("app.actions.handlers.state_manager", mock_state_manager)
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

@pytest.mark.asyncio
@pytest.mark.parametrize("error", [
    VectronicForbiddenException(Exception("403"), "Unauthorized access"),
    VectronicNotFoundException(Exception("404"), "Not found"),
    httpx.HTTPStatusError(
        "Server error",
        request=httpx.Request("GET", "https://api.vectronic-wildlife.com/v2/collar/1/gps"),
        response=httpx.Response(status_code=500, request=httpx.Request("GET", "https://api.vectronic-wildlife.com/v2/collar/1/gps")),
    ),
])
async def test_action_fetch_collar_observations_collar_warning_is_throttled(mocker, mock_state_manager, error):
    mocker.patch("app.services.activity_logger.publish_event", new=AsyncMock())
    # Another run already reported this collar within the window.
    mock_state_manager.set_if_absent.return_value = async_return(False)
    mocker.patch("app.actions.handlers.state_manager", mock_state_manager)
    mock_log_action_activity = mocker.patch("app.actions.handlers.log_action_activity", new_callable=AsyncMock)
    mocker.patch("app.actions.handlers.client.get_observations", new=AsyncMock(side_effect=error))
    integration = MagicMock(id=1, base_url=None)
    config = PullCollarObservationsConfig(start="2024-01-01T00:00:00", collar_id=1, collar_key="K")

    result = await action_fetch_collar_observations(integration, config)

    assert result == {"observations_extracted": 0}
    mock_log_action_activity.assert_not_awaited()
    throttle_kwargs = mock_state_manager.set_if_absent.call_args[1]
    assert throttle_kwargs["action_id"] == "fetch_collar_observations"
    assert throttle_kwargs["source_id"].endswith("-warning-1")
    assert throttle_kwargs["ttl_seconds"] == 3600


@pytest.mark.asyncio
async def test_action_fetch_collar_observations_collar_warning_fails_open(mocker, mock_state_manager):
    mocker.patch("app.services.activity_logger.publish_event", new=AsyncMock())
    mock_state_manager.set_if_absent.side_effect = Exception("redis down")
    mocker.patch("app.actions.handlers.state_manager", mock_state_manager)
    mock_log_action_activity = mocker.patch("app.actions.handlers.log_action_activity", new_callable=AsyncMock)
    mocker.patch(
        "app.actions.handlers.client.get_observations",
        new=AsyncMock(side_effect=VectronicNotFoundException(Exception("404"), "Not found")),
    )
    integration = MagicMock(id=1, base_url=None)
    config = PullCollarObservationsConfig(start="2024-01-01T00:00:00", collar_id=1, collar_key="K")

    result = await action_fetch_collar_observations(integration, config)

    assert result == {"observations_extracted": 0}
    mock_log_action_activity.assert_awaited_once()


@pytest.mark.asyncio
async def test_action_fetch_collar_observations_collar_warning_releases_throttle_on_publish_failure(mocker, mock_state_manager):
    mocker.patch("app.services.activity_logger.publish_event", new=AsyncMock())
    mock_state_manager.delete_state.return_value = async_return(None)
    mocker.patch("app.actions.handlers.state_manager", mock_state_manager)
    mocker.patch(
        "app.actions.handlers.log_action_activity", new=AsyncMock(side_effect=Exception("pubsub down"))
    )
    mocker.patch(
        "app.actions.handlers.client.get_observations",
        new=AsyncMock(side_effect=VectronicNotFoundException(Exception("404"), "Not found")),
    )
    integration = MagicMock(id=1, base_url=None)
    config = PullCollarObservationsConfig(start="2024-01-01T00:00:00", collar_id=1, collar_key="K")

    with pytest.raises(Exception, match="pubsub down"):
        await action_fetch_collar_observations(integration, config)

    # The next run must be able to publish the warning that never arrived.
    mock_state_manager.delete_state.assert_called_once_with(
        integration_id=integration.id,
        action_id="fetch_collar_observations",
        source_id="not-found-warning-1",
    )


@pytest.mark.asyncio
async def test_action_fetch_collar_observations_collar_warning_no_release_when_not_reserved(mocker, mock_state_manager):
    # Fail-open path: the throttle store was down, so there is no reservation to release.
    mocker.patch("app.services.activity_logger.publish_event", new=AsyncMock())
    mock_state_manager.set_if_absent.side_effect = Exception("redis down")
    mocker.patch("app.actions.handlers.state_manager", mock_state_manager)
    mocker.patch(
        "app.actions.handlers.log_action_activity", new=AsyncMock(side_effect=Exception("pubsub down"))
    )
    mocker.patch(
        "app.actions.handlers.client.get_observations",
        new=AsyncMock(side_effect=VectronicNotFoundException(Exception("404"), "Not found")),
    )
    integration = MagicMock(id=1, base_url=None)
    config = PullCollarObservationsConfig(start="2024-01-01T00:00:00", collar_id=1, collar_key="K")

    with pytest.raises(Exception, match="pubsub down"):
        await action_fetch_collar_observations(integration, config)

    mock_state_manager.delete_state.assert_not_called()


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
