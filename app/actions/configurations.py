import pydantic

from datetime import datetime, timezone
from typing import List

from app.actions.core import PullActionConfiguration, InternalActionConfiguration
from app.services.errors import ConfigurationNotFound
from app.services.utils import find_config_for_action, FieldWithUIOptions


class PullObservationsConfig(PullActionConfiguration):
    files: List[str] = FieldWithUIOptions(
        ...,
        title="XML Files",
        description="List of XML files to be processed",
    )
    default_lookback_hours: int = 12


class PullCollarObservationsConfig(PullActionConfiguration, InternalActionConfiguration):
    start: datetime
    collar_id: int
    collar_key: str

    @pydantic.validator('start', always=True)
    def parse_time_string(cls, v):
        if not v.tzinfo:
            return v.replace(tzinfo=timezone.utc)
        return v


def get_pull_config(integration):
    # Look for the login credentials, needed for any action
    pull_config = find_config_for_action(
        configurations=integration.configurations,
        action_id="pull_observations"
    )
    if not pull_config:
        raise ConfigurationNotFound(
            f"Authentication settings for integration {str(integration.id)} "
            f"are missing. Please fix the integration setup in the portal."
        )
    return PullObservationsConfig.parse_obj(pull_config.data)
