import logging
import httpx
import pydantic
import datetime
import stamina
import hashlib

from math import ceil
from typing import Optional
from app.services.state import IntegrationStateManager


logger = logging.getLogger(__name__)
state_manager = IntegrationStateManager()


PROTRACK_ERROR_CODE_EXPIRED_TOKEN = 10012
PROTRACK_MAX_OBSERVATIONS_RESPONSE = 1000


# Pydantic models
class DeviceResponse(pydantic.BaseModel):
    simcard: Optional[str]
    platenumber: Optional[str]
    iccid: Optional[str]
    userduetime: Optional[datetime.datetime]
    onlinetime: Optional[datetime.datetime]
    activatedtime: Optional[datetime.datetime]
    imei: str
    devicename: str
    devicetype: Optional[str]
    platformduetime: Optional[datetime.datetime]

    # Create a validator to include utc info to datetime fields
    @pydantic.validator('userduetime', 'onlinetime', 'activatedtime', 'platformduetime', pre=True, always=True)
    def clean_datetime(cls, v):
        if v:
            return datetime.datetime.fromtimestamp(v, datetime.timezone.utc)
        return v


class PlaybackResponse(pydantic.BaseModel):
    longitude: float
    latitude: float
    gpstime: datetime.datetime
    speed: int
    course: int


class ProTrackUnauthorizedException(Exception):
    def __init__(self, error: Exception, message: str, status_code=401):
        self.status_code = status_code
        self.message = message
        self.error = error
        super().__init__(f"'{self.status_code}: {self.message}, Error: {self.error}'")


def generate_md5_hash(input_string):
    md5_hash = hashlib.md5()
    md5_hash.update(input_string.encode('utf-8'))
    return md5_hash.hexdigest()


async def get_token(integration, base_url, auth):
    auth_url = f"{base_url}/authorization"
    async with httpx.AsyncClient(timeout=120) as session:
        token = await state_manager.get_state(
            integration_id=integration.id,
            action_id="pull_observations",
            source_id="token"
        )
        if not token:
            token = await get_auth_response(integration.id, auth_url, auth)
            if not token:
                message = f"Failed to authenticate with integration {integration.id} using {auth}"
                logger.error(message)
                raise ProTrackUnauthorizedException(message)
            await state_manager.set_state(
                integration_id=integration.id,
                action_id="pull_observations",
                source_id="token",
                state={"access_token": token}
            )
        else:
            token = token.get("access_token")
        return token


@stamina.retry(on=httpx.HTTPError, wait_initial=4.0, wait_jitter=5.0, wait_max=32.0)
async def get_auth_response(integration_id, url, auth):
    async with httpx.AsyncClient(timeout=120) as session:
        logger.info(f"-- Getting auth token for integration ID: {integration_id} Account: {auth.account} --")

        now_utc = datetime.datetime.now(datetime.timezone.utc)
        epoch_time = ceil(now_utc.timestamp())

        password_hash = generate_md5_hash(auth.password.get_secret_value())
        signature = generate_md5_hash(f"{password_hash}{epoch_time}")

        params = {
            "time": epoch_time,
            "account": auth.account,
            "signature": signature
        }

        response = await session.get(url, params=params)
        if response.is_error:  # Log response body on 4xx or 5xx
            logger.error(f"Error 'get_auth_response'. Response body: {response.text}")
        response.raise_for_status()
        parsed_response = response.json()
        if parsed_response:
            code = parsed_response.get('code')
            if code != 0:
                logger.error(f"{url} returned error: {parsed_response.get('message')}")
                return None
            return parsed_response["record"].get('access_token')
        else:
            return response.text


@stamina.retry(on=httpx.HTTPError, wait_initial=4.0, wait_jitter=5.0, wait_max=32.0)
async def get_devices(integration, base_url, auth):
    async with httpx.AsyncClient(timeout=120) as session:
        logger.info(f"-- Getting devices for integration ID: {integration.id} Account: {auth.account} --")

        devices_url = f"{base_url}/device/list"

        token = await get_token(integration, base_url, auth)

        params = {
            "access_token": token
        }

        response = await session.get(devices_url, params=params)
        if response.is_error:
            logger.error(f"Error 'get_devices'. Response body: {response.text}")
        response.raise_for_status()
        parsed_response = response.json()
        if parsed_response:
            code = parsed_response.get('code')
            if code != 0:
                if code == PROTRACK_ERROR_CODE_EXPIRED_TOKEN:
                    # Token expired, remove it from state and retry
                    await state_manager.delete_state(
                        integration_id=integration.id,
                        action_id="pull_observations",
                        source_id="token"
                    )
                    return await get_devices(integration, base_url, auth)
                logger.error(f"{devices_url} returned error: {parsed_response.get('message')}")
                return None
            return [DeviceResponse.parse_obj(item) for item in parsed_response.get('record')]
        else:
            return response.text


@stamina.retry(on=httpx.HTTPError, wait_initial=4.0, wait_jitter=5.0, wait_max=32.0)
async def get_playback_observations(integration, base_url, config):
    async with httpx.AsyncClient(timeout=120) as session:
        extracted_obs = []

        playback_url = f"{base_url}/playback"
        has_data = True

        while has_data:
            logger.info(f"-- Getting playback observations for integration ID: {integration.id} Device: {config.imei} --")

            response = await session.get(playback_url, params=config.dict())
            if response.is_error:
                logger.error(f"Error 'get_playback_observations'. Response body: {response.text}")
            response.raise_for_status()
            parsed_response = response.json()
            if parsed_response:
                code = parsed_response.get('code')
                if code != 0:
                    if code == PROTRACK_ERROR_CODE_EXPIRED_TOKEN:
                        # Token expired, remove it from state and retry
                        await state_manager.delete_state(
                            integration_id=integration.id,
                            action_id="pull_observations",
                            source_id="token"
                        )
                        return await get_playback_observations(integration, base_url, config)
                    logger.error(f"{playback_url} returned error: {parsed_response.get('message')}")
                    return None
                obs = parsed_response.get('record').split(";") if parsed_response.get('record') else []
                extracted_obs.extend(obs)

                # Check if there is more data to fetch
                if len(obs) == PROTRACK_MAX_OBSERVATIONS_RESPONSE:
                    latest_timestamp = obs[-1].split(",")[2]
                    config.begintime = latest_timestamp
                elif len(obs) < PROTRACK_MAX_OBSERVATIONS_RESPONSE:
                    has_data = False
            else:
                return response.text

        extracted_obs = [obs.split(",") for obs in extracted_obs]
        keys = ["longitude", "latitude", "gpstime", "speed", "course"]
        parsed_obs = [PlaybackResponse.parse_obj(dict(zip(keys, obs))) for obs in extracted_obs]

        return parsed_obs
