import httpx
import logging

import app.actions.client as client

from math import ceil
from datetime import datetime, timedelta, timezone
from app.actions.configurations import AuthenticateConfig, PullObservationsConfig, PlaybackConfig, get_auth_config
from app.services.action_scheduler import trigger_action
from app.services.activity_logger import activity_logger
from app.services.gundi import send_observations_to_gundi
from app.services.state import IntegrationStateManager
from app.services.utils import generate_batches

logger = logging.getLogger(__name__)
state_manager = IntegrationStateManager()


PROTRACK_BASE_URL = "https://api.protrack365.com/api"


def transform(device, observation):
    device_info = device.copy()
    device_imei = device_info.pop("imei")
    device_name = device_info.pop("devicename")

    return {
        "source_name": device_name,
        "source": device_imei,
        "type": "tracking-device",
        "subject_type": "vehicle",
        "recorded_at": observation.gpstime,
        "location": {
            "lat": observation.latitude,
            "lon": observation.longitude
        },
        "additional": {"speed": observation.speed, "course": observation.course, **device_info}
    }


async def action_auth(integration, action_config: AuthenticateConfig):
    logger.info(f"Executing 'auth' action with integration ID {integration.id} and action_config {action_config}...")

    base_url = integration.base_url or PROTRACK_BASE_URL
    auth_url = f"{base_url}/authorization"

    try:
        token = await client.get_auth_response(integration.id, auth_url, action_config)
        if not token:
            logger.error(f"Failed to authenticate with integration {integration.id} using {action_config}")
            return {"valid_credentials": False, "message": "Bad credentials"}
        return {"valid_credentials": True, "token": token}
    except httpx.HTTPStatusError as e:
        return {"error": True, "status_code": e.response.status_code}


@activity_logger()
async def action_pull_observations(integration, action_config: PullObservationsConfig):
    logger.info(f"Executing 'pull_observations' action with integration ID {integration.id} and action_config {action_config}...")

    base_url = integration.base_url or PROTRACK_BASE_URL
    auth_config = get_auth_config(integration)

    try:
        token = await client.get_token(integration, base_url, auth_config)
        devices = await client.get_devices(integration, base_url, auth_config)
        if devices:
            logger.info(f"Found {len(devices)} devices for integration {integration.id} Account: {auth_config.account}")
            now = datetime.now(timezone.utc)
            devices_triggered = 0
            for device in devices:
                logger.info(f"Triggering 'playback' action for device {device.imei} to extract observations...")
                device_state = await state_manager.get_state(
                    integration_id=integration.id,
                    action_id="pull_observations",
                    source_id=device.imei
                )
                if not device_state:
                    logger.info(f"Setting initial lookback days for device {device.imei} to {action_config.default_lookback_days}")
                    begin_time = ceil((now - timedelta(days=action_config.default_lookback_days)).timestamp())
                else:
                    logger.info(f"Setting begin time for device {device.imei} to {device_state.get('updated_at')}")
                    begin_time = device_state.get("updated_at")

                config = {
                    "access_token": token,
                    "device_info": device.dict(),
                    "imei": device.imei,
                    "begintime": begin_time,
                    "endtime": ceil(now.timestamp())
                }
                parsed_config = PlaybackConfig.parse_obj(config)
                await trigger_action(integration.id, "playback", config=parsed_config)
                devices_triggered += 1
            return {"devices_triggered": devices_triggered}
        else:
            logger.warning(f"No devices found for integration {integration.id} Account: {auth_config.account}")
            return {"devices_triggered": 0}
    except client.ProTrackUnauthorizedException as e:
        message = f"Failed to authenticate with integration {integration.id} using {auth_config}. Exception: {e}"
        logger.exception(message)
        raise client.ProTrackUnauthorizedException(e, message)


@activity_logger()
async def action_playback(integration, action_config: PlaybackConfig):
    logger.info(f"Executing action 'playback' for integration ID {integration.id} and action_config {action_config}...")

    base_url = integration.base_url or PROTRACK_BASE_URL
    observations_extracted = 0

    try:
        observations = await client.get_playback_observations(integration, base_url, action_config)
        if observations:
            logger.info(f"Extracted {len(observations)} observations for device {action_config.imei}")
            transformed_data = [transform(action_config.device_info, obs) for obs in observations]

            for i, batch in enumerate(generate_batches(transformed_data, 200)):
                logger.info(f'Sending observations batch #{i}: {len(batch)} observations. Device: {action_config.imei}')
                response = await send_observations_to_gundi(observations=batch, integration_id=integration.id)
                observations_extracted += len(response)

            # Save latest device updated_at
            latest_gpstime = max(observations, key=lambda obs: obs.gpstime).gpstime
            state = {"updated_at": ceil(latest_gpstime.timestamp())}

            await state_manager.set_state(
                integration_id=integration.id,
                action_id="pull_observations",
                state=state,
                source_id=action_config.imei
            )

            return {"observations_extracted": observations_extracted}
        else:
            logger.warning(f"No observations found for device {action_config.imei}")
            return {"observations_extracted": 0}
    except httpx.HTTPStatusError as e:
        message = f"Error while executing 'playback' for integration {integration.id}. Exception: {e}"
        logger.exception(message)
        raise e
    except client.ProTrackUnauthorizedException as e:
        message = f"Failed to authenticate with integration {integration.id}. Exception: {e}"
        logger.exception(message)
        raise client.ProTrackUnauthorizedException(e, message)
