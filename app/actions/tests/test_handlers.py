import pytest
import httpx

from math import ceil
from datetime import datetime, timezone, timedelta
from app.actions.client import DeviceResponse, PlaybackResponse
from app.actions.handlers import action_auth, action_pull_observations, action_playback
from app.actions.configurations import AuthenticateConfig, PullObservationsConfig, PlaybackConfig


@pytest.mark.asyncio
async def test_action_auth_success(mocker):
    mocker.patch("app.actions.client.get_auth_response", return_value="fake_token")
    integration = mocker.Mock(id=1, base_url=None)
    action_config = AuthenticateConfig.parse_obj({"account": "user", "password": "pass"})

    result = await action_auth(integration, action_config)
    assert result == {"valid_credentials": True, "token": "fake_token"}

@pytest.mark.asyncio
async def test_action_auth_bad_credentials(mocker):
    mocker.patch("app.actions.client.get_auth_response", return_value=None)
    integration = mocker.Mock(id=1, base_url=None)
    action_config = AuthenticateConfig.parse_obj({"account": "user", "password": "pass"})

    result = await action_auth(integration, action_config)
    assert result == {"valid_credentials": False, "message": "Bad credentials"}

@pytest.mark.asyncio
async def test_action_pull_observations_triggers_playback_action(mocker, integration_v2, mock_publish_event):
    mocker.patch("app.actions.client.get_token", return_value="fake_token")
    mocker.patch("app.actions.client.get_devices", return_value=[
        DeviceResponse.parse_obj({"imei": "12345", "devicename": "device"})
    ])
    mock_now = mocker.patch("app.actions.handlers.datetime")
    mock_now.now.return_value = datetime.now(timezone.utc)
    mocker.patch("app.services.state.IntegrationStateManager.get_state", return_value=None)
    mocker.patch("app.services.activity_logger.publish_event", mock_publish_event)
    mocker.patch("app.services.action_runner.publish_event", mock_publish_event)
    mocker.patch("app.services.action_scheduler.trigger_action", return_value=None)
    mock_trigger_action = mocker.patch("app.services.action_runner.execute_action", return_value=None)

    integration = integration_v2

    # Modify auth config
    integration.configurations[2].data = {"account": "user", "password": "pass"}

    action_config = PullObservationsConfig(default_lookback_days=5)

    result = await action_pull_observations(integration, action_config)
    assert result == {"devices_triggered": 1}

    expected_config = PlaybackConfig(
        access_token="fake_token",
        device_info=DeviceResponse.parse_obj({"imei": "12345", "devicename": "device"}).dict(),
        imei="12345",
        begintime=ceil((mock_now.now.return_value - timedelta(days=action_config.default_lookback_days)).timestamp()),
        endtime=ceil(mock_now.now.return_value.timestamp())
    )

    mock_trigger_action.assert_called_once_with(
        integration_id=integration.id,
        action_id="playback",
        config_overrides=expected_config.dict()
    )

@pytest.mark.asyncio
async def test_action_pull_observations_no_devices(mocker, integration_v2, mock_publish_event):
    mocker.patch("app.actions.client.get_token", return_value="fake_token")
    mocker.patch("app.actions.client.get_devices", return_value=[])
    mocker.patch("app.services.activity_logger.publish_event", mock_publish_event)
    mocker.patch("app.services.action_runner.publish_event", mock_publish_event)
    integration = integration_v2

    # Modify auth config
    integration.configurations[2].data = {"account": "user", "password": "pass"}

    action_config = PullObservationsConfig(default_lookback_days=5)

    result = await action_pull_observations(integration, action_config)
    assert result == {"devices_triggered": 0}

@pytest.mark.asyncio
async def test_action_pull_observations_auth_failure(mocker, integration_v2, mock_publish_event):
    mocker.patch("app.actions.client.get_token", side_effect=Exception("Auth failed"))
    mocker.patch("app.services.activity_logger.publish_event", mock_publish_event)
    mocker.patch("app.services.action_runner.publish_event", mock_publish_event)
    integration = integration_v2

    # Modify auth config
    integration.configurations[2].data = {"account": "user", "password": "pass"}
    action_config = PullObservationsConfig(default_lookback_days=5)

    with pytest.raises(Exception, match="Auth failed"):
        await action_pull_observations(integration, action_config)

@pytest.mark.asyncio
async def test_action_playback_extracts_observations(
        mocker,
        integration_v2,
        mock_publish_event,
        mock_gundi_client_v2_class,
        mock_gundi_sensors_client_class,
        mock_get_gundi_api_key
):
    mocker.patch("app.actions.client.get_playback_observations", return_value=[
        PlaybackResponse.parse_obj({"gpstime": datetime.now(timezone.utc), "latitude": 0, "longitude": 0, "speed": 0, "course": 0})
    ])
    mocker.patch("app.services.activity_logger.publish_event", mock_publish_event)
    mocker.patch("app.services.action_runner.publish_event", mock_publish_event)
    mocker.patch("app.services.utils.generate_batches", return_value=[[{"name": "device", "source": "12345", "type": "tracking-device", "subject_type": "vehicle", "recorded_at": datetime.now(timezone.utc), "location": {"lat": 0, "lon": 0}, "additional": {"speed": 0, "course": 0}}]])
    mocker.patch("app.services.gundi.GundiClient", mock_gundi_client_v2_class)
    mocker.patch("app.services.gundi.GundiDataSenderClient", mock_gundi_sensors_client_class)
    mocker.patch("app.services.gundi._get_gundi_api_key", mock_get_gundi_api_key)
    mocker.patch("app.services.state.IntegrationStateManager.set_state", return_value=None)

    integration = integration_v2
    action_config = PlaybackConfig(access_token="fake_token", device_info={"imei": "12345", "devicename": "device"}, imei="12345", begintime=0, endtime=0)

    result = await action_playback(integration, action_config)
    assert result == {"observations_extracted": 2}

@pytest.mark.asyncio
async def test_action_playback_no_observations(mocker, integration_v2, mock_publish_event):
    mocker.patch("app.actions.client.get_playback_observations", return_value=[])
    mocker.patch("app.services.activity_logger.publish_event", mock_publish_event)
    mocker.patch("app.services.action_runner.publish_event", mock_publish_event)

    integration = integration_v2
    action_config = PlaybackConfig(access_token="fake_token", device_info={"imei": "12345"}, imei="12345", begintime=0, endtime=0)

    result = await action_playback(integration, action_config)
    assert result == {"observations_extracted": 0}

@pytest.mark.asyncio
async def test_action_playback_http_error(mocker, integration_v2, mock_publish_event):
    mocker.patch("app.actions.client.get_playback_observations", side_effect=httpx.HTTPStatusError("Error", request=None, response=None))
    mocker.patch("app.services.activity_logger.publish_event", mock_publish_event)
    mocker.patch("app.services.action_runner.publish_event", mock_publish_event)

    integration = integration_v2
    action_config = PlaybackConfig(access_token="fake_token", device_info={"imei": "12345"}, imei="12345", begintime=0, endtime=0)

    with pytest.raises(httpx.HTTPStatusError):
        await action_playback(integration, action_config)
