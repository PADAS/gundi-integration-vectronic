import httpx
import logging

import base64
import tempfile

# import app.actions.client as client

from datetime import datetime, timedelta, timezone
from app.actions.configurations import AuthenticateConfig, PullObservationsConfig, get_auth_config
from app.services.action_scheduler import trigger_action
from app.services.activity_logger import activity_logger
from app.services.gundi import send_observations_to_gundi
from app.services.state import IntegrationStateManager
from app.services.utils import generate_batches

logger = logging.getLogger(__name__)
state_manager = IntegrationStateManager()


@activity_logger()
async def action_pull_observations(integration, action_config: PullObservationsConfig):
    logger.info(f"Executing 'pull_observations' action with integration ID {integration.id} and action_config {action_config}...")

    for index, base64_file in enumerate(action_config.files):
        try:
            # Decode the base64 content
            decoded_content = base64.b64decode(base64_file)

            # Create a temporary file
            with tempfile.NamedTemporaryFile(delete=False, suffix=".xml") as temp_file:
                temp_file.write(decoded_content)
                temp_file_path = temp_file.name

            # Read and print the file content
            with open(temp_file_path, "r") as file:
                file_content = file.read()
                print(f"Content of file {index + 1}:\n{file_content}")

        except Exception as e:
            logger.error(f"Failed to process file {index + 1}: {e}")

    return {"status": "success", "message": "Files processed successfully."}
