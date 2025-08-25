import logging
import httpx
import pydantic

from datetime import datetime, timezone
from app.services.state import IntegrationStateManager


logger = logging.getLogger(__name__)
state_manager = IntegrationStateManager()


class VectronicObservation(pydantic.BaseModel):
    idCollar: int
    acquisitionTime: datetime
    originCode: str
    ecefX: int
    ecefY: int
    ecefZ: int
    latitude: float
    longitude: float
    height: int
    dop: float
    mainVoltage: float
    backupVoltage: float
    temperature: float

    @pydantic.validator('acquisitionTime', always=True)
    def parse_time_string(cls, v):
        if not v.tzinfo:
            return v.replace(tzinfo=timezone.utc)
        return v


class VectronicNotFoundException(Exception):
    def __init__(self, error: Exception, message: str, status_code=404):
        self.status_code = status_code
        self.message = message
        self.error = error
        super().__init__(f"'{self.status_code}: {self.message}, Error: {self.error}'")


class VectronicForbiddenException(Exception):
    def __init__(self, error: Exception, message: str, status_code=403):
        self.status_code = status_code
        self.message = message
        self.error = error
        super().__init__(f"'{self.status_code}: {self.message}, Error: {self.error}'")


class VectronicBadRequestException(Exception):
    def __init__(self, error: Exception, message: str, status_code=400):
        self.status_code = status_code
        self.message = message
        self.error = error
        super().__init__(f"'{self.status_code}: {self.message}, Error: {self.error}'")


async def get_observations(integration, base_url, config):
    async with httpx.AsyncClient(timeout=httpx.Timeout(connect=10.0, read=30.0, write=15.0, pool=5.0)) as session:
        logger.info(f"-- Getting observations for integration ID: {integration.id} Collar ID: {config.collar_id} --")

        url = f"{base_url}/v2/collar/{config.collar_id}/gps"

        params = {
            "collarkey": config.collar_key,
            "afterScts": config.start.isoformat().split("+")[0]
        }

        try:
            response = await session.get(url, params=params)
            if response.is_error:
                logger.error(f"Error 'get_observations'. Response body: {response.text}")
            response.raise_for_status()
            parsed_response = response.json()
            if parsed_response:
                return [VectronicObservation.parse_obj(item) for item in parsed_response]
            else:
                return response.text
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 403:
                raise VectronicForbiddenException(e, "Unauthorized access")
            elif e.response.status_code == 404:
                raise VectronicNotFoundException(e, "Not found")
            raise e
