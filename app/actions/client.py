import logging
import re
import httpx
import pydantic

from datetime import datetime, timezone
from typing import Optional


logger = logging.getLogger(__name__)


_COLLAR_KEY_PATTERN = re.compile(r"(collarkey=)[^&\s\"']+", re.IGNORECASE)


def redact_collar_key(text: str) -> str:
    return _COLLAR_KEY_PATTERN.sub(r"\1<redacted>", text)


class _RedactCollarKey(logging.Filter):
    """httpx logs every request URL at INFO, and Vectronic takes the collar key
    as a query parameter, so mask it before the record reaches any handler."""

    def filter(self, record):
        message = record.getMessage()
        if "collarkey=" in message.lower():
            record.msg = redact_collar_key(message)
            record.args = ()
        return True


logging.getLogger("httpx").addFilter(_RedactCollarKey())


class VectronicObservation(pydantic.BaseModel):
    id_collar: int = pydantic.Field(..., alias="idCollar")
    acquisition_time: datetime = pydantic.Field(..., alias="acquisitionTime")
    # Service-center timestamp: when Vectronic received the fix. The afterScts
    # query filters on this, so it is what the per-collar cursor must track.
    scts: Optional[datetime] = pydantic.Field(None, alias="scts")
    origin_code: Optional[str] = pydantic.Field(None, alias="originCode")
    ecef_x: Optional[int] = pydantic.Field(None, alias="ecefX")
    ecef_y: Optional[int] = pydantic.Field(None, alias="ecefY")
    ecef_z: Optional[int] = pydantic.Field(None, alias="ecefZ")
    latitude: Optional[float] = pydantic.Field(None, alias="latitude")
    longitude: Optional[float] = pydantic.Field(None, alias="longitude")
    height: Optional[int] = pydantic.Field(None, alias="height")
    dop: Optional[float] = pydantic.Field(None, alias="dop")
    main_voltage: Optional[float] = pydantic.Field(None, alias="mainVoltage")
    backup_voltage: Optional[float] = pydantic.Field(None, alias="backupVoltage")
    temperature: Optional[float] = pydantic.Field(None, alias="temperature")

    class Config:
        allow_population_by_field_name = True

    @pydantic.validator('acquisition_time', 'scts', always=True)
    def parse_time_string(cls, v):
        if v is None:
            return v
        if not v.tzinfo:
            return v.replace(tzinfo=timezone.utc)
        # The cursor is stored without an offset and reloaded as UTC, so any
        # offset must be converted here or the cursor shifts by that offset.
        return v.astimezone(timezone.utc)


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


async def get_observations(base_url: str, collar_id: int, collar_key: str, after_scts: datetime):
    async with httpx.AsyncClient(timeout=httpx.Timeout(connect=10.0, read=30.0, write=15.0, pool=5.0)) as session:
        logger.info(f"-- Getting observations for Collar ID: {collar_id} after {after_scts} --")

        url = f"{base_url}/v2/collar/{collar_id}/gps"

        params = {
            "collarkey": collar_key,
            "afterScts": after_scts.strftime("%Y-%m-%dT%H:%M:%S")
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
                logger.warning(f"-- No observations returned for Collar ID: {collar_id}: {response.text}  --")
                return []
        except httpx.HTTPStatusError as e:
            # The message embeds the request URL, collar key included, and
            # callers log it. Re-raise with the key masked and without the
            # original in the exception chain.
            safe_error = httpx.HTTPStatusError(redact_collar_key(str(e)), request=e.request, response=e.response)
            if e.response.status_code == 403:
                raise VectronicForbiddenException(safe_error, "Unauthorized access") from None
            elif e.response.status_code == 404:
                raise VectronicNotFoundException(safe_error, "Not found") from None
            raise safe_error from None
