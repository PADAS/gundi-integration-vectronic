import json
import logging
import pydantic

import app.actions.client as client

from gundi_core.schemas.v2 import LogLevel
from datetime import datetime, timedelta, timezone
from app.actions.configurations import PullObservationsConfig, PullCollarObservationsConfig
from app.services.action_scheduler import trigger_action
from app.services.activity_logger import activity_logger, log_action_activity
from app.services.gundi import send_observations_to_gundi
from app.services.state import IntegrationStateManager
from app.services.utils import generate_batches


logger = logging.getLogger(__name__)
state_manager = IntegrationStateManager()


VECTRONIC_BASE_URL = "https://api.vectronic-wildlife.com"


class CollarData(pydantic.BaseModel):
    collar_id: str = pydantic.Field(..., alias="collarID")
    collar_type: str = pydantic.Field(..., alias="collarType")
    com_id: str = pydantic.Field(..., alias="comID")
    com_type: str = pydantic.Field(..., alias="comType")
    key: str = pydantic.Field(..., alias="key")

    class Config:
        allow_population_by_field_name = True


def transform(observation):
    additional_info = {
        key: value for key, value in observation.dict().items() if value and key not in ["idCollar", "acquisitionTime", "latitude", "longitude"]
    }

    return {
        "source_name": observation.idCollar,
        "source": observation.idCollar,
        "type": "tracking-device",
        "subject_type": "wildlife",
        "recorded_at": observation.acquisitionTime,
        "location": {
            "lat": observation.latitude,
            "lon": observation.longitude
        },
        "additional": {
            **additional_info
        }
    }


@activity_logger()
async def action_pull_observations(integration, action_config: PullObservationsConfig):
    logger.info(f"Executing 'pull_observations' action with integration ID {integration.id} and action_config {action_config}...")

    collars_triggered = 0

    try:
        # Turn string JSON into list of dicts
        collars = json.loads(action_config.files)
    except json.JSONDecodeError as e:
        logger.error(f"Failed to decode collars JSON for integration ID {integration.id} and action_config {action_config}: {e}")
        raise e

    if not collars:
        logger.warning(f"No valid collars found for integration ID {integration.id} and action_config {action_config}")
        return {"status": "success", "collars_triggered": 0}

    try:
        for collar in collars:
            parsed_collar = CollarData.parse_obj(collar["parsedData"])
            logger.info(f"Triggering 'action_fetch_collar_observations' action for collar {parsed_collar.collar_id} to extract observations...")
            now = datetime.now(timezone.utc)
            device_state = await state_manager.get_state(
                integration_id=integration.id,
                action_id="pull_observations",
                source_id=parsed_collar.collar_id
            )
            if not device_state:
                logger.info(f"Setting initial lookback hours for device {parsed_collar.collar_id} to {action_config.default_lookback_hours}")
                start = (now - timedelta(hours=action_config.default_lookback_hours)).strftime("%Y-%m-%dT%H:%M:%S")
            else:
                logger.info(f"Setting begin time for device {parsed_collar.collar_id} to {device_state.get('updated_at')}")
                start = device_state.get("updated_at")

            parsed_config = PullCollarObservationsConfig(
                start=start,
                collar_id=int(parsed_collar.collar_id),
                collar_key=parsed_collar.key
            )
            await trigger_action(integration.id, "fetch_collar_observations", config=parsed_config)
            collars_triggered += 1

    except Exception as e:
        logger.error(f"Failed to process collars from integration ID {integration.id} and action_config {action_config}")
        raise e

    return {"status": "success", "collars_triggered": collars_triggered}


@activity_logger()
async def action_fetch_collar_observations(integration, action_config: PullCollarObservationsConfig):
    logger.info(f"Executing 'fetch_collar_observations' action with integration ID {integration.id} and action_config {action_config}...")

    base_url = integration.base_url or VECTRONIC_BASE_URL
    observations_extracted = 0

    try:
        observations = await client.get_observations(integration, base_url, action_config)
        if observations:
            logger.info(f"Extracted {len(observations)} observations for collar {action_config.collar_id}")

            transformed_data = [transform(ob) for ob in observations]

            for i, batch in enumerate(generate_batches(transformed_data, 200)):
                logger.info(f'Sending observations batch #{i}: {len(batch)} observations. Collar: {action_config.collar_id}')
                response = await send_observations_to_gundi(observations=batch, integration_id=integration.id)
                observations_extracted += len(response)

            # Save latest device updated_at
            latest_time = max(observations, key=lambda obs: obs.acquisitionTime).acquisitionTime
            state = {"updated_at": latest_time.strftime("%Y-%m-%dT%H:%M:%S")}

            await state_manager.set_state(
                integration_id=integration.id,
                action_id="pull_observations",
                state=state,
                source_id=str(action_config.collar_id)
            )

            return {"observations_extracted": observations_extracted}
        else:
            return {"observations_extracted": 0}
    except client.VectronicForbiddenException as e:
        message = f"Unauthorized response from Vectronic with integration {integration.id} using {action_config}. Exception: {e}"
        logger.exception(message)
        await log_action_activity(
            integration_id=integration.id,
            action_id="pull_observations",
            level=LogLevel.WARNING,
            title="Unauthorized access (bad collar key and/or collar ID)",
            data={"message": message, "data": action_config}
        )
        return {"observations_extracted": 0}
    except client.VectronicNotFoundException as e:
        message = f"Collar ID {action_config.collar_id} not found. Integration {integration.id} using {action_config}. Exception: {e}"
        logger.exception(message)
        await log_action_activity(
            integration_id=integration.id,
            action_id="pull_observations",
            level=LogLevel.WARNING,
            title=f"Collar ID {action_config.collar_id} not found.",
            data={"message": message}
        )
        return {"observations_extracted": 0}
    except Exception as e:
        message = f"Failed to fetch observations for collar {action_config.collar_id} from integration ID {integration.id}. Exception: {e}"
        logger.exception(message)
        await log_action_activity(
            integration_id=integration.id,
            action_id="pull_observations",
            level=LogLevel.WARNING,
            title=f"Failed to fetch observations for collar {action_config.collar_id}.",
            data={"message": message}
        )
        return {"observations_extracted": 0}
