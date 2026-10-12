import asyncio
import pytest
from datetime import datetime, timezone
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
    checkpoints_by_receive_time,
    get_collar_key,
    CollarData,
    transform,
)
from app.actions.configurations import PullObservationsConfig, PullCollarObservationsConfig
from app.conftest import async_return


@pytest.fixture(autouse=True)
def collar_key_lookup(mocker):
    # Fetch tests use a bare MagicMock integration with no pull config.
    return mocker.patch("app.actions.handlers.get_collar_key", return_value="K")

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
    # The key is looked up by the fetch handler, never carried in the message.
    assert "collar_key" not in configs[0].dict()

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
async def test_action_fetch_collar_observations_no_observations(mocker, mock_state_manager):
    mocker.patch("app.actions.handlers.state_manager", mock_state_manager)
    mock_get_obs = mocker.patch("app.actions.handlers.client.get_observations", new_callable=AsyncMock)
    mocker.patch("app.services.activity_logger.publish_event", new=AsyncMock())
    mocker.patch("app.services.action_runner.publish_event", new=AsyncMock())
    mocker.patch("app.services.action_scheduler.publish_event", new=AsyncMock())
    integration = MagicMock(id=1, base_url=None)
    config = PullCollarObservationsConfig(start="2024-01-01T00:00:00", collar_id=1)
    mock_get_obs.return_value = []
    result = await action_fetch_collar_observations(integration, config)
    assert result["observations_extracted"] == 0

def make_observation(acquisition_time, latitude=1.0, longitude=2.0, collar_id=123, scts=None):
    return VectronicObservation(
        id_collar=collar_id,
        acquisition_time=acquisition_time,
        scts=scts,
        latitude=latitude,
        longitude=longitude,
    )


@pytest.mark.asyncio
async def test_action_fetch_collar_observations_summarizes_invalid_locations(mocker, caplog, mock_state_manager):
    mocker.patch("app.services.activity_logger.publish_event", new=AsyncMock())
    integration = MagicMock(id=1, base_url=None)
    config = PullCollarObservationsConfig(start="2024-01-01T00:00:00", collar_id=123)
    invalid_obs = [
        make_observation(f"2024-01-01T0{i}:00:00", latitude=None, longitude=None)
        for i in range(1, 6)
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
    config = PullCollarObservationsConfig(start="2022-09-01T00:00:00", collar_id=123)
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
    # A fix whose receive time (here, acquisition time, since scts is missing)
    # is older than the start is dropped, so it can neither be re-reported nor
    # pull the cursor back.
    mocker.patch("app.services.activity_logger.publish_event", new=AsyncMock())
    integration = MagicMock(id=1, base_url=None)
    config = PullCollarObservationsConfig(start="2026-10-12T00:00:00", collar_id=123)
    late_ob = make_observation("2022-09-26T16:01:39", latitude=None, longitude=None)
    mocker.patch("app.actions.handlers.state_manager", mock_state_manager)
    mock_log_action_activity = mocker.patch("app.actions.handlers.log_action_activity", new_callable=AsyncMock)
    mocker.patch("app.actions.handlers.client.get_observations", new=AsyncMock(return_value=[late_ob]))

    result = await action_fetch_collar_observations(integration, config)

    assert result == {"observations_extracted": 0}
    mock_state_manager.set_state.assert_not_called()
    mock_log_action_activity.assert_not_awaited()


@pytest.mark.asyncio
async def test_action_fetch_collar_observations_cursor_tracks_receive_time(mocker, mock_state_manager):
    # Fixes acquired in 2020 but received recently pass afterScts=start; the
    # cursor must move past their receive time or every run re-fetches them.
    mocker.patch("app.services.activity_logger.publish_event", new=AsyncMock())
    integration = MagicMock(id=1, base_url=None)
    config = PullCollarObservationsConfig(start="2026-10-12T00:00:00", collar_id=37590)
    late_obs = [
        make_observation("2020-07-22T13:01:39", latitude=None, longitude=None, scts="2026-10-12T03:10:00"),
        make_observation("2020-07-22T14:01:39", latitude=None, longitude=None, scts="2026-10-12T03:15:27"),
    ]
    mocker.patch("app.actions.handlers.state_manager", mock_state_manager)
    mocker.patch("app.actions.handlers.log_action_activity", new_callable=AsyncMock)
    mocker.patch("app.actions.handlers.client.get_observations", new=AsyncMock(return_value=late_obs))

    await action_fetch_collar_observations(integration, config)

    assert mock_state_manager.set_state.call_args[1]["state"] == {"updated_at": "2026-10-12T03:15:27"}


@pytest.mark.asyncio
async def test_action_fetch_collar_observations_drops_fixes_at_or_before_cursor(mocker, mock_state_manager):
    # Fixes received at exactly the cursor were processed by an earlier run; an
    # inclusive afterScts returns them again, and they must not be re-reported.
    mocker.patch("app.services.activity_logger.publish_event", new=AsyncMock())
    integration = MagicMock(id=1, base_url=None)
    config = PullCollarObservationsConfig(start="2026-06-22T18:55:47", collar_id=37590)
    seen_obs = [
        make_observation("2020-07-22T13:01:39", latitude=None, longitude=None, scts="2026-06-22T18:55:47"),
        make_observation("2020-07-22T14:01:39", latitude=None, longitude=None, scts="2026-06-22T18:55:47"),
    ]
    mocker.patch("app.actions.handlers.state_manager", mock_state_manager)
    mock_log_action_activity = mocker.patch("app.actions.handlers.log_action_activity", new_callable=AsyncMock)
    mocker.patch("app.actions.handlers.client.get_observations", new=AsyncMock(return_value=seen_obs))

    result = await action_fetch_collar_observations(integration, config)

    assert result == {"observations_extracted": 0}
    mock_log_action_activity.assert_not_awaited()
    mock_state_manager.set_state.assert_not_called()


@pytest.mark.asyncio
async def test_action_fetch_collar_observations_mixed_valid_and_invalid(mocker, mock_state_manager):
    mocker.patch("app.services.activity_logger.publish_event", new=AsyncMock())
    integration = MagicMock(id=1, base_url=None)
    config = PullCollarObservationsConfig(start="2024-01-01T00:00:00", collar_id=123)
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
    summary, completion = [c[1] for c in mock_log_action_activity.call_args_list]
    assert summary["data"]["skipped_count"] == 1
    assert completion["title"] == "Collar ID 123: sent 1 observation(s) to Gundi."
    assert mock_state_manager.set_state.call_args[1]["state"] == {"updated_at": "2024-01-01T02:00:00"}


@pytest.mark.asyncio
async def test_action_fetch_collar_observations_summary_failure_does_not_block_delivery(mocker, mock_state_manager):
    mocker.patch("app.services.activity_logger.publish_event", new=AsyncMock())
    integration = MagicMock(id=1, base_url=None)
    config = PullCollarObservationsConfig(start="2024-01-01T00:00:00", collar_id=123)
    valid_ob = make_observation("2024-01-01T01:00:00")
    invalid_ob = make_observation("2024-01-01T02:00:00", latitude=None)
    mocker.patch("app.actions.handlers.state_manager", mock_state_manager)
    mock_send = mocker.patch(
        "app.actions.handlers.send_observations_to_gundi", new=AsyncMock(return_value=[{"object_id": "x"}])
    )
    mocker.patch("app.actions.handlers.log_action_activity", new=AsyncMock(side_effect=Exception("pubsub down")))
    mocker.patch("app.actions.handlers.client.get_observations", new=AsyncMock(return_value=[valid_ob, invalid_ob]))

    result = await action_fetch_collar_observations(integration, config)

    assert result == {"observations_extracted": 1}
    mock_send.assert_awaited_once()
    assert mock_state_manager.set_state.call_args[1]["state"] == {"updated_at": "2024-01-01T02:00:00"}


@pytest.mark.asyncio
async def test_action_fetch_collar_observations_exception_sends_error_activity_log(mocker, mock_state_manager):
    mocker.patch("app.actions.handlers.state_manager", mock_state_manager)
    mock_get_obs = mocker.patch("app.actions.handlers.client.get_observations", new_callable=AsyncMock)
    mock_log_action_activity = mocker.patch("app.actions.handlers.log_action_activity", new_callable=AsyncMock)
    mocker.patch("app.services.activity_logger.publish_event", new=AsyncMock())
    integration = MagicMock(id=1, base_url=None)
    config = PullCollarObservationsConfig(start="2024-01-01T00:00:00", collar_id=1)
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
    config = PullCollarObservationsConfig(start="2024-01-01T00:00:00", collar_id=1)
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
    config = PullCollarObservationsConfig(start="2024-01-01T00:00:00", collar_id=1)
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
    config = PullCollarObservationsConfig(start="2024-01-01T00:00:00", collar_id=1)
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
    config = PullCollarObservationsConfig(start="2024-01-01T00:00:00", collar_id=1)

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
    config = PullCollarObservationsConfig(start="2024-01-01T00:00:00", collar_id=1)

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
    config = PullCollarObservationsConfig(start="2024-01-01T00:00:00", collar_id=1)

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
    config = PullCollarObservationsConfig(start="2024-01-01T00:00:00", collar_id=1)

    with pytest.raises(Exception, match="pubsub down"):
        await action_fetch_collar_observations(integration, config)

    mock_state_manager.delete_state.assert_not_called()


@pytest.mark.asyncio
async def test_action_fetch_collar_observations_resumes_from_saved_cursor(mocker, mock_state_manager):
    # A redelivered message still carries the 2022 start it was published with.
    mocker.patch("app.services.activity_logger.publish_event", new=AsyncMock())
    mock_state_manager.get_state.return_value = async_return({"updated_at": "2026-06-22T18:55:47"})
    mocker.patch("app.actions.handlers.state_manager", mock_state_manager)
    mock_get_obs = mocker.patch("app.actions.handlers.client.get_observations", new=AsyncMock(return_value=[]))
    integration = MagicMock(id=1, base_url=None)
    config = PullCollarObservationsConfig(start="2022-11-05T05:00:10", collar_id=37590)

    await action_fetch_collar_observations(integration, config)

    assert mock_get_obs.call_args[1]["after_scts"].isoformat() == "2026-06-22T18:55:47+00:00"
    assert mock_get_obs.call_args[1]["collar_key"] == "K"


@pytest.mark.asyncio
async def test_action_fetch_collar_observations_keeps_newer_message_start(mocker, mock_state_manager):
    mocker.patch("app.services.activity_logger.publish_event", new=AsyncMock())
    mock_state_manager.get_state.return_value = async_return({"updated_at": "2026-01-01T00:00:00"})
    mocker.patch("app.actions.handlers.state_manager", mock_state_manager)
    mock_get_obs = mocker.patch("app.actions.handlers.client.get_observations", new=AsyncMock(return_value=[]))
    integration = MagicMock(id=1, base_url=None)
    config = PullCollarObservationsConfig(start="2026-10-12T00:00:00", collar_id=1)

    await action_fetch_collar_observations(integration, config)

    assert mock_get_obs.call_args[1]["after_scts"].isoformat() == "2026-10-12T00:00:00+00:00"


@pytest.mark.asyncio
async def test_action_fetch_collar_observations_saves_cursor_after_each_batch(mocker, mock_state_manager):
    # The run is cut off after the first batch: its progress is kept, and the
    # summary is not posted because the second batch never completed.
    mocker.patch("app.services.activity_logger.publish_event", new=AsyncMock())
    mocker.patch("app.actions.handlers.OBSERVATION_BATCH_SIZE", 2)
    mocker.patch("app.actions.handlers.state_manager", mock_state_manager)
    obs = [
        make_observation("2024-01-01T01:00:00", scts="2024-01-02T01:00:00"),
        make_observation("2024-01-01T02:00:00", latitude=None, scts="2024-01-02T02:00:00"),
        make_observation("2024-01-01T03:00:00", scts="2024-01-02T03:00:00"),
        make_observation("2024-01-01T04:00:00", scts="2024-01-02T04:00:00"),
    ]
    mocker.patch("app.actions.handlers.client.get_observations", new=AsyncMock(return_value=list(reversed(obs))))
    mock_send = mocker.patch(
        "app.actions.handlers.send_observations_to_gundi",
        new=AsyncMock(side_effect=[[{"object_id": "a"}], Exception("request timeout")]),
    )
    mock_log_action_activity = mocker.patch("app.actions.handlers.log_action_activity", new_callable=AsyncMock)
    integration = MagicMock(id=1, base_url=None)
    config = PullCollarObservationsConfig(start="2024-01-01T00:00:00", collar_id=123)

    await action_fetch_collar_observations(integration, config)

    # Observations are sent in receive-time order, so the saved cursor is a safe resume point.
    first_batch = mock_send.call_args_list[0][1]["observations"]
    assert [o["recorded_at"].hour for o in first_batch] == [1]
    mock_state_manager.set_state.assert_called_once()
    assert mock_state_manager.set_state.call_args[1]["state"] == {"updated_at": "2024-01-02T02:00:00"}
    # Only the generic error log; no invalid-location summary for the interrupted run.
    assert all("invalid location" not in c[1]["title"] for c in mock_log_action_activity.call_args_list)


@pytest.mark.asyncio
async def test_action_fetch_collar_observations_caps_requests_for_bulk_uploads(mocker, mock_state_manager):
    # Collar 37590: 1,373 fixes share one scts. They must go out in requests of
    # at most the batch size (a single 1,373-fix request got HTTP 504), and the
    # cursor moves past the group only after every request succeeds.
    mocker.patch("app.services.activity_logger.publish_event", new=AsyncMock())
    mocker.patch("app.actions.handlers.OBSERVATION_BATCH_SIZE", 2)
    mocker.patch("app.actions.handlers.state_manager", mock_state_manager)
    shared = [make_observation(f"2020-07-22T{h:02d}:00:00", scts="2026-06-22T18:55:47") for h in range(5)]
    mocker.patch("app.actions.handlers.client.get_observations", new=AsyncMock(return_value=shared))
    mock_send = mocker.patch(
        "app.actions.handlers.send_observations_to_gundi",
        new=AsyncMock(side_effect=lambda observations, integration_id: [{}] * len(observations)),
    )
    integration = MagicMock(id=1, base_url=None)
    config = PullCollarObservationsConfig(start="2022-11-05T05:00:10", collar_id=37590)

    result = await action_fetch_collar_observations(integration, config)

    assert result == {"observations_extracted": 5}
    assert [len(c[1]["observations"]) for c in mock_send.call_args_list] == [2, 2, 1]
    mock_state_manager.set_state.assert_called_once()
    assert mock_state_manager.set_state.call_args[1]["state"] == {"updated_at": "2026-06-22T18:55:47"}


@pytest.mark.asyncio
async def test_action_fetch_collar_observations_bulk_upload_failure_keeps_cursor(mocker, mock_state_manager):
    # A request inside a shared-receive-time group fails: the cursor must not
    # move past the group, or its unsent fixes would be skipped for good.
    mocker.patch("app.services.activity_logger.publish_event", new=AsyncMock())
    mocker.patch("app.actions.handlers.OBSERVATION_BATCH_SIZE", 2)
    mocker.patch("app.actions.handlers.state_manager", mock_state_manager)
    mocker.patch("app.actions.handlers.log_action_activity", new_callable=AsyncMock)
    shared = [make_observation(f"2020-07-22T{h:02d}:00:00", scts="2026-06-22T18:55:47") for h in range(5)]
    mocker.patch("app.actions.handlers.client.get_observations", new=AsyncMock(return_value=shared))
    mocker.patch(
        "app.actions.handlers.send_observations_to_gundi",
        new=AsyncMock(side_effect=[[{}, {}], Exception("HTTP 504")]),
    )
    integration = MagicMock(id=1, base_url=None)
    config = PullCollarObservationsConfig(start="2022-11-05T05:00:10", collar_id=37590)

    await action_fetch_collar_observations(integration, config)

    mock_state_manager.set_state.assert_not_called()


def test_checkpoints_by_receive_time_never_split_a_shared_receive_time():
    shared = [make_observation(f"2020-07-22T{h:02d}:00:00", scts="2026-06-22T18:55:47") for h in range(5)]
    later = [make_observation("2020-07-23T00:00:00", scts="2026-06-23T00:00:00")]
    batches = list(checkpoints_by_receive_time(shared + later, 2))
    assert [len(b) for b in batches] == [5, 1]


def test_get_collar_key_reads_pull_config():
    files = json.dumps([
        {"parsedData": {"collarID": "37590", "collarType": "A", "comID": "X", "comType": "Y", "key": "KEY-37590"}},
        {"parsedData": {"collarID": "46530", "collarType": "A", "comID": "X", "comType": "Y", "key": "KEY-46530"}},
    ])
    pull_config = MagicMock(action=MagicMock(value="pull_observations"), data={"files": files})
    integration = MagicMock(configurations=[pull_config])
    # Imported before the autouse fixture patches the module attribute, so this is the real lookup.
    assert get_collar_key(integration, 46530) == "KEY-46530"
    assert get_collar_key(integration, 99999) is None


@pytest.mark.asyncio
async def test_action_fetch_collar_observations_skips_collar_missing_from_config(mocker, mock_state_manager, collar_key_lookup):
    collar_key_lookup.return_value = None
    mocker.patch("app.services.activity_logger.publish_event", new=AsyncMock())
    mocker.patch("app.actions.handlers.state_manager", mock_state_manager)
    mock_get_obs = mocker.patch("app.actions.handlers.client.get_observations", new_callable=AsyncMock)
    integration = MagicMock(id=1, base_url=None)
    config = PullCollarObservationsConfig(start="2024-01-01T00:00:00", collar_id=1)

    result = await action_fetch_collar_observations(integration, config)

    assert result == {"observations_extracted": 0}
    mock_get_obs.assert_not_awaited()


def test_pull_collar_observations_config_ignores_legacy_collar_key():
    # Messages published before the key was dropped still carry it.
    config = PullCollarObservationsConfig.parse_obj(
        {"start": "2024-01-01T00:00:00", "collar_id": 1, "collar_key": "LEGACY"}
    )
    assert "collar_key" not in config.dict()
    assert "LEGACY" not in repr(config)


@pytest.mark.asyncio
async def test_save_cursor_does_not_rewind_a_newer_saved_cursor(mocker, mock_state_manager):
    # A faster concurrent run already saved t10; this slower run reaches t4.
    from app.actions.handlers import save_cursor
    mock_state_manager.get_state.return_value = async_return({"updated_at": "2024-01-10T00:00:00"})
    mocker.patch("app.actions.handlers.state_manager", mock_state_manager)

    await save_cursor(1, 123, datetime(2024, 1, 4, tzinfo=timezone.utc))

    mock_state_manager.set_state.assert_not_called()


@pytest.mark.asyncio
async def test_action_fetch_collar_observations_drops_fixes_without_scts_at_or_before_cursor(mocker, mock_state_manager):
    mocker.patch("app.services.activity_logger.publish_event", new=AsyncMock())
    mocker.patch("app.actions.handlers.state_manager", mock_state_manager)
    old_ob = make_observation("2024-01-01T00:00:00", latitude=None)
    mocker.patch("app.actions.handlers.client.get_observations", new=AsyncMock(return_value=[old_ob]))
    mock_log_action_activity = mocker.patch("app.actions.handlers.log_action_activity", new_callable=AsyncMock)
    integration = MagicMock(id=1, base_url=None)
    config = PullCollarObservationsConfig(start="2024-01-01T00:00:00", collar_id=123)

    result = await action_fetch_collar_observations(integration, config)

    assert result == {"observations_extracted": 0}
    mock_log_action_activity.assert_not_awaited()


def test_pull_observations_config_keeps_collar_keys_out_of_events_and_repr():
    files = json.dumps([{"parsedData": {"collarID": "1", "collarType": "A", "comID": "X", "comType": "Y", "key": "SECRETKEY123"}}])
    config = PullObservationsConfig(files=files, default_lookback_hours=12)
    assert "files" not in config.dict()
    assert "SECRETKEY123" not in repr(config)
    assert config.files == files
    # The portal still renders the field.
    assert "files" in PullObservationsConfig.schema()["properties"]


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


def test_transform_excludes_scts_from_additional():
    obs = VectronicObservation.parse_obj({
        "idCollar": 1,
        "acquisitionTime": "2024-01-01T00:00:00",
        "scts": "2024-01-01T00:05:00",
        "latitude": 1.0,
        "longitude": 2.0,
    })
    assert "scts" not in transform(obs)["additional"]

def test_collar_data_ok():
    data = {"collarID": "1", "collarType": "A", "comID": "X", "comType": "Y", "key": "K"}
    model = CollarData.parse_obj(data)
    assert model.collar_id == "1"

def test_collar_data_parsing_error():
    data = {"collarType": "A", "comID": "X", "comType": "Y", "key": "K"}
    with pytest.raises(ValidationError) as exc_info:
        CollarData.parse_obj(data)
    assert "collarID" in str(exc_info.value)


@pytest.mark.asyncio
async def test_action_fetch_collar_observations_with_nothing_new_publishes_no_activity(mocker, mock_state_manager):
    # Runs once per collar per schedule tick: an "action completed" entry for
    # every empty run buried the useful entries in the activity log.
    mock_publish_event = mocker.patch("app.services.activity_logger.publish_event", new=AsyncMock())
    mocker.patch("app.actions.handlers.state_manager", mock_state_manager)
    mocker.patch("app.actions.handlers.client.get_observations", new=AsyncMock(return_value=[]))
    integration = MagicMock(id=1, base_url=None)
    config = PullCollarObservationsConfig(start="2024-01-01T00:00:00", collar_id=35245)

    result = await action_fetch_collar_observations(integration=integration, action_config=config)

    assert result == {"observations_extracted": 0}
    mock_publish_event.assert_not_awaited()


@pytest.mark.asyncio
async def test_action_fetch_collar_observations_logs_one_entry_naming_the_collar(mocker, mock_state_manager):
    mock_publish_event = mocker.patch("app.services.activity_logger.publish_event", new=AsyncMock())
    mocker.patch("app.actions.handlers.state_manager", mock_state_manager)
    observations = [make_observation(f"2024-01-01T0{i}:00:00", collar_id=35245) for i in range(1, 4)]
    mocker.patch("app.actions.handlers.client.get_observations", new=AsyncMock(return_value=observations))
    mocker.patch(
        "app.actions.handlers.send_observations_to_gundi",
        new=AsyncMock(side_effect=lambda observations, integration_id: observations),
    )
    integration = MagicMock(id=1, base_url=None)
    config = PullCollarObservationsConfig(start="2024-01-01T00:00:00", collar_id=35245)

    result = await action_fetch_collar_observations(integration=integration, action_config=config)

    assert result == {"observations_extracted": 3}
    mock_publish_event.assert_awaited_once()
    payload = mock_publish_event.call_args.kwargs["event"].payload
    assert payload.title == "Collar ID 35245: sent 3 observation(s) to Gundi."
    assert payload.level == LogLevel.INFO
    assert payload.data == {"collar_id": 35245, "observations_extracted": 3}


@pytest.mark.asyncio
async def test_action_fetch_collar_observations_completion_entry_failure_does_not_fail_the_run(mocker, mock_state_manager):
    mocker.patch("app.services.activity_logger.publish_event", new=AsyncMock())
    mocker.patch("app.actions.handlers.state_manager", mock_state_manager)
    mocker.patch("app.actions.handlers.client.get_observations", new=AsyncMock(return_value=[make_observation("2024-01-01T01:00:00")]))
    mocker.patch("app.actions.handlers.send_observations_to_gundi", new=AsyncMock(return_value=[{"object_id": "x"}]))
    mocker.patch("app.actions.handlers.log_action_activity", new=AsyncMock(side_effect=Exception("pubsub down")))
    integration = MagicMock(id=1, base_url=None)
    config = PullCollarObservationsConfig(start="2024-01-01T00:00:00", collar_id=123)

    result = await action_fetch_collar_observations(integration, config)

    # The observations are already in Gundi and the cursor is saved.
    assert result == {"observations_extracted": 1}
    assert mock_state_manager.set_state.call_args[1]["state"] == {"updated_at": "2024-01-01T01:00:00"}


@pytest.mark.asyncio
@pytest.mark.parametrize("error, failure_title", [
    (Exception("gundi down"), "Failed to fetch observations for collar 123."),
    (
        httpx.HTTPStatusError(
            "Server error",
            request=httpx.Request("POST", "https://gundi.example/v2/observations/"),
            response=httpx.Response(status_code=502, request=httpx.Request("POST", "https://gundi.example/v2/observations/")),
        ),
        # Gundi's error, not reported as Vectronic's.
        "Gundi returned HTTP 502 while sending observations for collar 123.",
    ),
])
async def test_action_fetch_collar_observations_reports_what_was_sent_before_a_failure(mocker, mock_state_manager, error, failure_title):
    mocker.patch("app.services.activity_logger.publish_event", new=AsyncMock())
    mocker.patch("app.actions.handlers.state_manager", mock_state_manager)
    mocker.patch("app.actions.handlers.OBSERVATION_BATCH_SIZE", 2)
    observations = [make_observation(f"2024-01-01T0{i}:00:00", collar_id=123) for i in range(1, 5)]
    mocker.patch("app.actions.handlers.client.get_observations", new=AsyncMock(return_value=observations))
    # The first checkpoint reaches Gundi; the second request fails.
    mocker.patch(
        "app.actions.handlers.send_observations_to_gundi",
        new=AsyncMock(side_effect=[[{"object_id": "a"}, {"object_id": "b"}], error]),
    )
    mock_log_action_activity = mocker.patch("app.actions.handlers.log_action_activity", new_callable=AsyncMock)
    integration = MagicMock(id=1, base_url=None)
    config = PullCollarObservationsConfig(start="2024-01-01T00:00:00", collar_id=123)

    result = await action_fetch_collar_observations(integration, config)

    assert result == {"observations_extracted": 2}
    titles = [c[1]["title"] for c in mock_log_action_activity.call_args_list]
    assert titles == [failure_title, "Collar ID 123: sent 2 observation(s) to Gundi."]


@pytest.mark.asyncio
async def test_action_fetch_collar_observations_gundi_errors_do_not_share_the_vectronic_throttle(mocker, mock_state_manager):
    mocker.patch("app.services.activity_logger.publish_event", new=AsyncMock())
    mocker.patch("app.actions.handlers.state_manager", mock_state_manager)
    mocker.patch("app.actions.handlers.client.get_observations", new=AsyncMock(return_value=[make_observation("2024-01-01T01:00:00")]))
    request = httpx.Request("POST", "https://gundi.example/v2/observations/")
    mocker.patch(
        "app.actions.handlers.send_observations_to_gundi",
        new=AsyncMock(side_effect=httpx.HTTPStatusError("Bad gateway", request=request, response=httpx.Response(502, request=request))),
    )
    mocker.patch("app.actions.handlers.log_action_activity", new_callable=AsyncMock)
    integration = MagicMock(id=1, base_url=None)
    config = PullCollarObservationsConfig(start="2024-01-01T00:00:00", collar_id=123)

    await action_fetch_collar_observations(integration, config)

    assert mock_state_manager.set_if_absent.call_args[1]["source_id"] == "gundi-error-warning-123"


@pytest.mark.asyncio
async def test_action_fetch_collar_observations_cancelled_run_skips_the_sent_entry(mocker, mock_state_manager):
    # Publishing retries with backoff; a cancelled run (timeout, shutdown)
    # must not hold the worker past its deadline to report a count.
    mocker.patch("app.services.activity_logger.publish_event", new=AsyncMock())
    mocker.patch("app.actions.handlers.state_manager", mock_state_manager)
    mocker.patch("app.actions.handlers.OBSERVATION_BATCH_SIZE", 1)
    observations = [make_observation(f"2024-01-01T0{i}:00:00", collar_id=123) for i in range(1, 3)]
    mocker.patch("app.actions.handlers.client.get_observations", new=AsyncMock(return_value=observations))
    mocker.patch(
        "app.actions.handlers.send_observations_to_gundi",
        new=AsyncMock(side_effect=[[{"object_id": "a"}], asyncio.CancelledError()]),
    )
    mock_log_action_activity = mocker.patch("app.actions.handlers.log_action_activity", new_callable=AsyncMock)
    integration = MagicMock(id=1, base_url=None)
    config = PullCollarObservationsConfig(start="2024-01-01T00:00:00", collar_id=123)

    with pytest.raises(asyncio.CancelledError):
        await action_fetch_collar_observations(integration, config)

    mock_log_action_activity.assert_not_awaited()
