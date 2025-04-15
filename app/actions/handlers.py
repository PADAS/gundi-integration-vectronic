import httpx
import logging
import base64

import app.actions.client as client

from datetime import datetime, timedelta, timezone
from app.actions.configurations import PullObservationsConfig, PullCollarObservationsConfig
from app.services.action_scheduler import trigger_action
from app.services.activity_logger import activity_logger
from app.services.gundi import send_observations_to_gundi
from app.services.state import IntegrationStateManager
from app.services.utils import generate_batches

from lxml import etree


logger = logging.getLogger(__name__)
state_manager = IntegrationStateManager()


VECTRONIC_BASE_URL = "https://api.vectronic-wildlife.com"


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

    for index, base64_file in enumerate(action_config.files):
        try:
            # Decode the base64 content
            decoded_content = base64.b64decode(base64_file)

            # Validate if the file is an XML file
            try:
                root = etree.fromstring(decoded_content)
            except etree.XMLSyntaxError as e:
                raise client.VectronicXMLParseException(e, f"File {index + 1} is not a valid XML file.")

            # Extract <collar ID> and <key>
            collar_element = root.find(".//collar")
            if collar_element is None:
                raise client.VectronicXMLParseException(Exception("Missing <collar> element in the XML."), "Missing <collar> element in the XML.")

            collar_id = collar_element.get("ID")
            if not collar_id:
                raise client.VectronicXMLParseException(Exception("Missing 'ID' attribute in <collar> element."), "Missing 'ID' attribute in <collar> element.")

            key_element = collar_element.find("key")
            if key_element is None or not key_element.text:
                raise client.VectronicXMLParseException(Exception("Missing <key> element or its value in the XML."), "Missing <key> element or its value in the XML.")

            collar_key = key_element.text

            logger.info(f"Triggering 'action_fetch_collar_observations' action for collar {collar_id} to extract observations...")
            now = datetime.now(timezone.utc)
            device_state = await state_manager.get_state(
                integration_id=integration.id,
                action_id="pull_observations",
                source_id=collar_id
            )
            if not device_state:
                logger.info(f"Setting initial lookback hours for device {collar_id} to {action_config.default_lookback_hours}")
                start = (now - timedelta(hours=action_config.default_lookback_hours)).strftime("%Y-%m-%dT%H:%M:%S")
            else:
                logger.info(f"Setting begin time for device {collar_id} to {device_state.get('updated_at')}")
                start = device_state.get("updated_at")

            parsed_config = PullCollarObservationsConfig(
                start=start,
                collar_id=int(collar_id),
                collar_key=collar_key
            )
            await trigger_action(integration.id, "fetch_collar_observations", config=parsed_config)
            collars_triggered += 1

        except client.VectronicXMLParseException as e:
            logger.exception(f"The file #{index + 1} included in config is invalid. Exception: {e}")
            raise e
        except Exception as e:
            logger.error(f"Failed to process file {index + 1}: {e}")
            raise e

    return {"status": "success", "collars_triggered": collars_triggered}


@activity_logger()
async def action_fetch_collar_observations(integration, action_config: PullCollarObservationsConfig):
    logger.info(f"Executing 'fetch_collar_observations' action with integration ID {integration.id} and action_config {action_config}...")

    base_url = integration.base_url or VECTRONIC_BASE_URL
    observations_extracted = 0

    try:
        observations = await client.get_observations(integration, base_url, action_config)
        if observations and isinstance(observations, list):
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
            logger.warning(f"No observations found for collar {action_config.collar_id}")
            return {"observations_extracted": 0}
    except (client.VectronicForbiddenException, client.VectronicNotFoundException) as e:
        message = f"Failed to authenticate with integration {integration.id} using {action_config}. Exception: {e}"
        logger.exception(message)
        raise e
