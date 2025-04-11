import pydantic

from typing import List

from app.actions.core import GenericActionConfiguration, AuthActionConfiguration, PullActionConfiguration, ExecutableActionMixin, InternalActionConfiguration
from app.services.errors import ConfigurationNotFound
from app.services.utils import find_config_for_action, UIOptions, FieldWithUIOptions, GlobalUISchemaOptions


class ProcessFileConfig(GenericActionConfiguration, ExecutableActionMixin):
    file: pydantic.FilePath


class AuthenticateConfig(AuthActionConfiguration, ExecutableActionMixin):
    pass


class PullObservationsConfig(PullActionConfiguration):
    files: List[str] = FieldWithUIOptions(
        ...,
        title="XML Files",
        description="List of XML files to be processed",
    )


def get_auth_config(integration):
    # Look for the login credentials, needed for any action
    auth_config = find_config_for_action(
        configurations=integration.configurations,
        action_id="auth"
    )
    if not auth_config:
        raise ConfigurationNotFound(
            f"Authentication settings for integration {str(integration.id)} "
            f"are missing. Please fix the integration setup in the portal."
        )
    return AuthenticateConfig.parse_obj(auth_config.data)


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
