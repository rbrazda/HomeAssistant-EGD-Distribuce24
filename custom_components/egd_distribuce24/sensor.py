# sensor.py
import logging
import requests
import datetime
from datetime import timedelta, timezone
from datetime import datetime as dt_module
from dateutil import tz as dateutil_tz
import os
import asyncio

from homeassistant.components.sensor import (
    SensorDeviceClass,
    SensorStateClass,
    SensorEntity,
)
from homeassistant.const import UnitOfEnergy, UnitOfPower, EVENT_HOMEASSISTANT_START
from homeassistant.util import Throttle
from homeassistant.util import dt as dt_util
from homeassistant.helpers import entity_registry as er
from homeassistant.core import HomeAssistant
from homeassistant.components.recorder import DOMAIN as DOMAIN_RECORDER 
from homeassistant.components import recorder
from homeassistant.components.recorder.statistics import (
    get_last_statistics,
    async_add_external_statistics,
)

# Import your custom consts (must exist as in original integration)
from .const import (
    DOMAIN, 
    TOKEN_URL,
    DATA_URL,
    CONF_CLIENT_ID,
    CONF_CLIENT_SECRET,
    CONF_EAN,
    CONF_DAYS_OFFSET,
    CONF_MAX_FETCH_DAYS,
    CONF_CREATE_CONSUMPTION_SENSORS,
    CONF_CREATE_PRODUCTION_SENSORS,
    PROFILE_CONSUMPTION_POWER,
    PROFILE_PRODUCTION_POWER,
    PROFILE_CONSUMPTION_ENERGY,
    PROFILE_PRODUCTION_ENERGY,
    DEFAULT_SCAN_INTERVAL,
    TIMEZONE_PRAGUE,
    API_GRANT_TYPE,
    API_SCOPE,
    API_PAGE_SIZE,
    ATTR_LAST_UPDATE_STATUS,
    ATTR_FIFTEEN_MINUTE_DATA,
    ATTR_API_DATA_POINTS_RECEIVED,
    ATTR_DATA_FETCHED_FOR_DATE_LOCAL,
    ATTR_DATA_RANGE_UTC_FROM,
    ATTR_DATA_RANGE_UTC_TO,
    ATTR_LAST_SUCCESSFUL_FETCH_UTC,
    ATTR_WATCHED_CONSUMER_SENSOR_ENTITY_ID,
    ATTR_CONSUMER_LAST_UPDATE_STATUS,
    ATTR_CONSUMER_LAST_SUCCESSFUL_FETCH_UTC,
    ATTR_CONSUMER_DATA_FETCHED_FOR_DATE_LOCAL,
    ATTR_CONSUMER_TOTAL_KWH,
    ATTR_LOOKUP_STATUS,
    PLATFORMS,
)

MIN_TIME_BETWEEN_UPDATES = DEFAULT_SCAN_INTERVAL
SCRIPT_VERSION = "1.3.7_exclude_null_max"

_LOGGER = logging.getLogger(__name__)
DEBUG_PREFIX = f"[{DOMAIN.upper()}_DEBUG]" 

LOG_DIR_BASE = "/config/logs" 
LOG_DIR = os.path.join(LOG_DIR_BASE, DOMAIN) 
LOG_FILE = os.path.join(LOG_DIR, f"{DOMAIN}.log") 

try:
    if not os.path.exists(LOG_DIR_BASE):
        os.makedirs(LOG_DIR_BASE, exist_ok=True)
    if not os.path.exists(LOG_DIR):
        os.makedirs(LOG_DIR, exist_ok=True)

    file_handler = logging.FileHandler(LOG_FILE, mode='a')
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(name)s - %(message)s')
    file_handler.setFormatter(formatter)

    logger_to_configure = logging.getLogger(f"custom_components.{DOMAIN}") 
    if not any(isinstance(h, logging.FileHandler) and getattr(h, 'baseFilename', None) == LOG_FILE for h in logger_to_configure.handlers):
        logger_to_configure.addHandler(file_handler)
        logger_to_configure.setLevel(logging.INFO) 
        logger_to_configure.propagate = False 
    
    _LOGGER.info(f"--- DEDICATED LOGGER FOR {DOMAIN} INITIALIZED. Logging to: {LOG_FILE} --- Version: {SCRIPT_VERSION} ---")

except Exception as e_log_setup:
    _LOGGER.error(f"Error setting up dedicated file logger for {DOMAIN}: {e_log_setup}. Falling back to standard HA logging.", exc_info=True)


async def async_setup_entry(hass: HomeAssistant, config_entry, async_add_entities):
    """Set up EGD-Distribuce24 sensors from a config entry."""
    _LOGGER.info(f"{DEBUG_PREFIX} async_setup_entry CALLED for {config_entry.title} (Script Version: {SCRIPT_VERSION})")

    entry_data = config_entry.data
    client_id = entry_data[CONF_CLIENT_ID]
    client_secret = entry_data[CONF_CLIENT_SECRET]
    ean = entry_data[CONF_EAN]
    days_offset = entry_data[CONF_DAYS_OFFSET]
    max_fetch_days = entry_data[CONF_MAX_FETCH_DAYS]
    create_consumption = entry_data.get(CONF_CREATE_CONSUMPTION_SENSORS, True)
    create_production = entry_data.get(CONF_CREATE_PRODUCTION_SENSORS, True)

    _LOGGER.info(f"{DEBUG_PREFIX} Config for EAN {ean}: DaysOffset={days_offset}, MaxFetch={max_fetch_days}, CreateConsumption={create_consumption}, CreateProduction={create_production}")

    hass.data.setdefault(DOMAIN, {})
    hass.data[DOMAIN].setdefault('entries', {})
    hass.data[DOMAIN]['entries'][ean] = {
        "entry_id": config_entry.entry_id,
        "client_id": client_id,
        "client_secret": client_secret
    }
    hass.data[DOMAIN].setdefault(f"{DOMAIN}_initial_recorder_delay_done", False)


    sensors_to_add = []
    consumption_power_sensor = None 

    try:
        _LOGGER.info(f"{DEBUG_PREFIX} Attempting to create sensor instances for EAN {ean}.")
        if create_consumption:
            _LOGGER.info(f"{DEBUG_PREFIX} Creating EGDPowerConsumptionSensor for EAN {ean}.")
            consumption_power_sensor = EGDPowerConsumptionSensor(hass, client_id, client_secret, ean, days_offset, max_fetch_days, config_entry.entry_id)
            sensors_to_add.append(consumption_power_sensor)
            _LOGGER.info(f"{DEBUG_PREFIX} Creating EGDEnergyConsumptionSensor for EAN {ean}.")
            consumption_energy_sensor = EGDEnergyConsumptionSensor(hass, client_id, client_secret, ean, days_offset, max_fetch_days, config_entry.entry_id)
            sensors_to_add.append(consumption_energy_sensor)
            _LOGGER.info(f"{DEBUG_PREFIX} Consumption sensors created and added to list for EAN {ean}.")

        if create_production:
            _LOGGER.info(f"{DEBUG_PREFIX} Creating EGDPowerProductionSensor for EAN {ean}.")
            production_power_sensor = EGDPowerProductionSensor(hass, client_id, client_secret, ean, days_offset, max_fetch_days, config_entry.entry_id)
            sensors_to_add.append(production_power_sensor)
            _LOGGER.info(f"{DEBUG_PREFIX} Creating EGDEnergyProductionSensor for EAN {ean}.")
            production_energy_sensor = EGDEnergyProductionSensor(hass, client_id, client_secret, ean, days_offset, max_fetch_days, config_entry.entry_id)
            sensors_to_add.append(production_energy_sensor)
            _LOGGER.info(f"{DEBUG_PREFIX} Production sensors created and added to list for EAN {ean}.")

        if consumption_power_sensor: 
            _LOGGER.info(f"{DEBUG_PREFIX} Creating EGDStatusSensor for EAN {ean}, watching {consumption_power_sensor.unique_id}.")
            status_sensor = EGDStatusSensor(hass, ean, days_offset, consumption_power_sensor.unique_id, config_entry.entry_id)
            sensors_to_add.append(status_sensor)
            _LOGGER.info(f"{DEBUG_PREFIX} Status sensor created and added to list for EAN {ean}.")
        elif not create_consumption and create_production: 
             _LOGGER.warning(f"{DEBUG_PREFIX} Status sensor not added for EAN {ean} as consumption sensors are disabled, but production sensors are enabled. Status sensor typically watches a consumption power sensor.")
        elif not create_consumption and not create_production:
            _LOGGER.info(f"{DEBUG_PREFIX} No data or status sensors will be added for EAN {ean} as both consumption and production are disabled.")
        
        _LOGGER.info(f"{DEBUG_PREFIX} Preparing to call async_add_entities for EAN {ean} with {len(sensors_to_add)} sensors. update_before_add=False")
        if sensors_to_add:
            for s_idx, s_inst in enumerate(sensors_to_add):
                _LOGGER.info(f"{DEBUG_PREFIX} Sensor to be added [{s_idx}]: {s_inst.name} (Unique ID: {s_inst.unique_id}), Type: {type(s_inst)}")
            async_add_entities(sensors_to_add, False) 
            _LOGGER.info(f"{DEBUG_PREFIX} Call to async_add_entities completed for EAN {ean}.")
        else:
            _LOGGER.info(f"{DEBUG_PREFIX} No sensors were configured for addition for EAN {ean}.")
        _LOGGER.info(f"{DEBUG_PREFIX} async_setup_entry FINISHED for EAN {ean}.")

    except Exception as e:
        _LOGGER.error(f"{DEBUG_PREFIX} Error during sensor instantiation or add_entities for EAN {ean}: {e}", exc_info=True)

class EGDBaseSensor(SensorEntity):
    """Base class for EGD data sensors."""

    _attr_device_class = SensorDeviceClass.ENERGY

    def __init__(self, hass: HomeAssistant, client_id: str, client_secret: str, ean: str,
                 days_offset: int, max_fetch_days: int, api_profile: str,
                 profile_friendly_name: str, entry_id: str,
                 is_value_direct_kwh: bool = False,
                 target_unit: str = UnitOfEnergy.KILO_WATT_HOUR,
                 target_state_class: SensorStateClass = SensorStateClass.TOTAL_INCREASING):
        _LOGGER.info(f"{DEBUG_PREFIX} EGDBaseSensor __init__ CALLED for profile: {api_profile}, EAN: {ean}")
        self.hass = hass
        self._client_id = client_id
        self._client_secret = client_secret
        self._ean = str(ean)
        self._days_offset = days_offset
        self._max_fetch_days = max_fetch_days
        self._api_profile = api_profile
        self._profile_friendly_name = profile_friendly_name
        self._entry_id = entry_id
        self._is_value_direct_kwh = is_value_direct_kwh
        self._target_unit = target_unit
        self._target_state_class = target_state_class

        self._session = requests.Session()
        self._state: float | None = None
        self._attributes: dict[str, any] = {}
        self._last_reset_datetime_utc: dt_module | None = None 
        self._last_processed_date: datetime.date | None = None

        self._attr_unique_id = f"{DOMAIN}_{self._ean}_{self._days_offset}_{self._api_profile.lower()}"
        day_desc_en = f"{self._days_offset}d ago (win {self._max_fetch_days}d)"
        self._attr_name = f"EGD {self._profile_friendly_name} {self._ean} ({day_desc_en})"
        _LOGGER.info(f"{DEBUG_PREFIX} EGDBaseSensor __init__ PARTIALLY DONE for: {self.name}, Unique ID: {self.unique_id}, Profile: {self._api_profile}")
        _LOGGER.info(f"{DEBUG_PREFIX} Initialized EGDBaseSensor (v{SCRIPT_VERSION}): {self.name}, Unique ID: {self.unique_id}, Direct kWh: {self._is_value_direct_kwh}, Unit: {self._target_unit}, StateClass: {self._target_state_class}")
        _LOGGER.info(f"{DEBUG_PREFIX} EGDBaseSensor __init__ FINISHED for: {self.name}")

    async def async_added_to_hass(self) -> None:
        """Handle entity which will be added."""
        await super().async_added_to_hass()
        _LOGGER.info(f"{DEBUG_PREFIX} EGDBaseSensor: {self.name} (UID: {self.unique_id}) was added to hass. Entity ID: {self.entity_id}")
        _LOGGER.info(f"{DEBUG_PREFIX} Current HASS state in async_added_to_hass for {self.name}: {self.hass.state} (type: {type(self.hass.state)})")

        delay_flag_name = f"{DOMAIN}_initial_recorder_delay_done"

        async def _async_schedule_first_update(event=None):
            """Helper to schedule the first update, potentially after a delay."""
            if event: 
                _LOGGER.info(f"{DEBUG_PREFIX} Home Assistant started event received for {self.entity_id if self.entity_id else self.name}.")
            
            # --- WAIT UNTIL HA IS RUNNING ---
            while self.hass.state != "RUNNING":
                _LOGGER.debug(f"{DEBUG_PREFIX} {self.name}: Waiting for Home Assistant state RUNNING (current: {self.hass.state})")
                await asyncio.sleep(3)

            # --- ENSURE WE WAIT FOR RECORDER TO BE AVAILABLE BEFORE IMPORT/WRITE ---
            # Only perform initial wait once per integration instance to avoid delays for multiple entities
            if not self.hass.data[DOMAIN].get(delay_flag_name, False):
                _LOGGER.info(f"{DEBUG_PREFIX} Performing one-time initial recorder wait before first {DOMAIN} sensor update for {self.name} to allow recorder to register.")
                # wait up to ~60s for recorder to show up (polling)
                for _ in range(12):
                    if DOMAIN_RECORDER in self.hass.config.components:
                        _LOGGER.info(f"{DEBUG_PREFIX} Recorder component detected for {DOMAIN}. Proceeding with first update for {self.name}.")
                        break
                    _LOGGER.debug(f"{DEBUG_PREFIX} Recorder not found yet. Sleeping briefly before re-checking for {self.name}.")
                    await asyncio.sleep(5)
                else:
                    _LOGGER.warning(f"{DEBUG_PREFIX} Recorder component ({DOMAIN_RECORDER}) not detected within wait timeout. First update will proceed, but statistics import might be skipped.")
                self.hass.data[DOMAIN][delay_flag_name] = True
                _LOGGER.info(f"{DEBUG_PREFIX} One-time recorder wait complete for {DOMAIN} sensors.")
            else:
                _LOGGER.info(f"{DEBUG_PREFIX} Initial recorder wait already performed this session for {DOMAIN}, proceeding with update for {self.name}.")

            if self.entity_id:
                _LOGGER.info(f"{DEBUG_PREFIX} Scheduling first data update for {self.entity_id} ({self.name}) via async_update_ha_state(True)")
                # Use async_create_task to schedule the update without blocking
                self.hass.async_create_task(self.async_update_ha_state(True))
            else:
                _LOGGER.warning(f"{DEBUG_PREFIX} {self.name} (UID: {self.unique_id}) cannot schedule first update directly after add: entity_id is STILL None. This is unexpected. Update will rely on polling interval.")

        if self.hass.state == "RUNNING":
            _LOGGER.info(f"{DEBUG_PREFIX} Home Assistant is already running (checked as string 'RUNNING') for {self.entity_id if self.entity_id else self.name}. Scheduling first update sequence.")
            # directly schedule the sequence
            self.hass.async_create_task(_async_schedule_first_update())
        else:
            _LOGGER.info(f"{DEBUG_PREFIX} Home Assistant not fully started (state: {self.hass.state}) for {self.entity_id if self.entity_id else self.name}. Listening for EVENT_HOMEASSISTANT_START.")
            self.hass.bus.async_listen_once(
                EVENT_HOMEASSISTANT_START, _async_schedule_first_update
            )

    @property
    def device_class(self) -> SensorDeviceClass | None:
        if self._target_unit == UnitOfEnergy.KILO_WATT_HOUR:
            return SensorDeviceClass.ENERGY
        elif self._target_unit == UnitOfPower.KILO_WATT:
            return SensorDeviceClass.POWER
        return None

    @property
    def unit_of_measurement(self) -> str | None:
        return self._target_unit

    @property
    def state_class(self) -> SensorStateClass | None:
        return self._target_state_class

    @property
    def device_info(self):
        try:
            return {
                "identifiers": {(DOMAIN, str(self._ean))},
                "name": f"EGD Device ({str(self._ean)})", 
                "manufacturer": "EGD CZ",
            }
        except Exception as e:
            _LOGGER.error(f"{DEBUG_PREFIX} Error generating device_info for {self.name}: {e}", exc_info=True)
            return None

    @property
    def state(self) -> float | None:
        if self._target_unit == UnitOfEnergy.KILO_WATT_HOUR and self._target_state_class == SensorStateClass.TOTAL:
            return 0.0 
        return self._state

    @property
    def extra_state_attributes(self) -> dict[str, any]:
        return self._attributes

    @property
    def last_reset(self) -> dt_module | None:
        # IMPORTANT: Home Assistant only allows last_reset for 'total' state_class.
        # Return last_reset only for SensorStateClass.TOTAL to avoid HA ValueError.
        if self._target_state_class == SensorStateClass.TOTAL:
            return self._last_reset_datetime_utc
        return None

    @Throttle(MIN_TIME_BETWEEN_UPDATES)
    async def async_update(self) -> None:
        entity_id_log = self.entity_id if hasattr(self, 'entity_id') and self.entity_id else "Not yet available"
        _LOGGER.info(f"{DEBUG_PREFIX} EGDBaseSensor: ASYNC_UPDATE called for {self.name} (Entity ID: {entity_id_log}, Profile: {self._api_profile})")
        await self.hass.async_add_executor_job(self._perform_update)

    def _perform_update(self) -> None:
        entity_id_log = self.entity_id if hasattr(self, 'entity_id') and self.entity_id else "Not available during _perform_update"
        _LOGGER.info(f"{DEBUG_PREFIX} EGDBaseSensor: _PERFORM_UPDATE started for {self.name} (Entity ID: {entity_id_log}, Profile: {self._api_profile})")

        if not self.hass:
            _LOGGER.error(f"{DEBUG_PREFIX} HASS object not available in _perform_update for {self.name}. Aborting update.")
            return

        access_token = self._get_access_token_from_api()
        if not access_token:
            self._state = None
            self._attributes[ATTR_LAST_UPDATE_STATUS] = 'Token Retrieval Failed'
            _LOGGER.warning(f"{DEBUG_PREFIX} _PERFORM_UPDATE failed for {self.name} due to token retrieval failure.")
            return
        
        from homeassistant.util import dt as dt_util
        from zoneinfo import ZoneInfo
        import datetime
        
        # 1️⃣ Načti časovou zónu z HA (nebo fallback)
        tz_name = getattr(self.hass.config, "time_zone", None) or "Europe/Prague"
        tz = ZoneInfo(tz_name)
        
        # 2️⃣ Vypočítej půlnoc ručně podle lokální zóny
        now_local = dt_util.now(tz)
        midnight_local = now_local.replace(hour=0, minute=0, second=0, microsecond=0)
        midnight_utc = midnight_local.astimezone(datetime.timezone.utc)
        
        # 3️⃣ Ulož a loguj
        self._last_reset_datetime_utc = midnight_utc
        _LOGGER.info(
            f"{DEBUG_PREFIX} Local midnight recalculated for {self.name}: "
            f"{midnight_local.isoformat()} (UTC {midnight_utc.isoformat()})"
        )

        local_tz = tz
        today_local = dt_util.now(local_tz).date()
        
        
        last_key = f"{self.unique_id}_last_processed"
        last_processed_date: datetime.date | None = self.hass.data[DOMAIN].get(last_key)

        data_found_for_any_day = False
        
        for i in range(self._max_fetch_days):
            target_date_local = today_local - timedelta(days=self._days_offset + i)

            # skip today's date (we want only complete past days)
            if target_date_local >= today_local:
                continue

            # if already processed this or earlier date, skip
            if last_processed_date and target_date_local <= last_processed_date:
                continue

            period_start_local = datetime.datetime.combine(
                target_date_local, datetime.time.min, tzinfo=local_tz
            )
            period_end_local = datetime.datetime.combine(
                target_date_local, datetime.time(23, 45), tzinfo=local_tz
            )

            processed_data = self._fetch_and_process_data_for_day(
                access_token, period_start_local, period_end_local, target_date_local
            )
            
            # --- SAFETY CHECK: skip import if all interval values are zero or None ---
            if not processed_data or all((v == 0 or v is None) for v in processed_data.values()):
                _LOGGER.warning(
                    f"{DEBUG_PREFIX} STATISTICS IMPORT SKIPPED for {self.entity_id}: "
                    f"All interval values are zero for {target_date}."
                )
                return 
            
            if processed_data["data_found"]:
                _LOGGER.info(f"{DEBUG_PREFIX} Data found for {self.name} for date {target_date_local.strftime('%Y-%m-%d')}. Processing...")
                
                if self._target_unit == UnitOfEnergy.KILO_WATT_HOUR and self._target_state_class == SensorStateClass.TOTAL:
                    self._attributes["daily_total_kwh"] = processed_data["total_kwh_for_day"]
                    self._state = 0.0 
                    # For SensorStateClass.TOTAL we return last_reset (already set above to local midnight UTC)
                    _LOGGER.info(f"{DEBUG_PREFIX} For energy sensor {self.name}, main state set to {self._state}, daily_total_kwh attribute: {self._attributes['daily_total_kwh']}, main last_reset: {self._last_reset_datetime_utc}")
                elif self._target_state_class == SensorStateClass.TOTAL_INCREASING: 
                    self._state = None      #1111 processed_data["total_kwh_for_day"] 
                    # last_reset is stored internally but NOT returned to HA unless state_class == TOTAL
                    # store it for internal diagnostics and logging
                    self._last_reset_datetime_utc = period_start_local.astimezone(timezone.utc)
                elif self._target_state_class == SensorStateClass.MEASUREMENT: 
                    if processed_data["detailed_intervals"]:
                        self._state = processed_data["detailed_intervals"][-1].get('value')
                    else:
                        self._state = None
                
                self._attributes.update(processed_data["attributes_to_set"])
                _LOGGER.info(f"{DEBUG_PREFIX} Data successfully fetched for '{self.name}'. Main state: {self._state}")
                data_found_for_any_day = True

                # --- Import statistics logic ---
                if not processed_data.get("data_found") or not processed_data.get("detailed_intervals"):
                    _LOGGER.warning(f"{DEBUG_PREFIX} STATISTICS IMPORT SKIPPED for {self.entity_id}: No data found from API for {target_date_local.strftime('%Y-%m-%d')}")
                    continue
                intervals = processed_data.get("detailed_intervals", [])
                if not processed_data.get("data_found") or not intervals:
                    _LOGGER.warning(f"{DEBUG_PREFIX} STATISTICS IMPORT SKIPPED for {self.entity_id}: No data found from API.")
                    continue
                
                # ✅ new check
                if all((i.get("value") or 0) == 0 for i in intervals):
                    _LOGGER.warning(f"{DEBUG_PREFIX} STATISTICS IMPORT SKIPPED for {self.entity_id}: All interval values are zero for {target_date_local.strftime('%Y-%m-%d')}.")
                    continue
                
                if self.entity_id and self._target_unit == UnitOfEnergy.KILO_WATT_HOUR:
                    _LOGGER.info(f"{DEBUG_PREFIX} STATISTICS IMPORT (v{SCRIPT_VERSION}): Starting for {self.entity_id} for date {target_date_local.strftime('%Y-%m-%d')}")
                    
                    recorder_loaded = DOMAIN_RECORDER in self.hass.config.components
                    import_service_exists = self.hass.services.has_service(DOMAIN_RECORDER, "import_statistics")
                    
                    _LOGGER.info(f"{DEBUG_PREFIX} STATISTICS IMPORT: Recorder loaded: {recorder_loaded}, Import service exists: {import_service_exists} for {self.entity_id}")

                    if not recorder_loaded:
                        _LOGGER.warning(f"{DEBUG_PREFIX} STATISTICS IMPORT: Recorder component ({DOMAIN_RECORDER}) not found. Skipping for {self.entity_id}.")
                    elif not import_service_exists:
                        _LOGGER.warning(f"{DEBUG_PREFIX} STATISTICS IMPORT: Service {DOMAIN_RECORDER}.import_statistics not found. Skipping for {self.entity_id}.")
                        if DOMAIN_RECORDER in self.hass.services.async_services(): 
                            _LOGGER.warning(f"{DEBUG_PREFIX} STATISTICS IMPORT: Available services for '{DOMAIN_RECORDER}': {list(self.hass.services.async_services()[DOMAIN_RECORDER].keys())}")
                        else:
                            _LOGGER.warning(f"{DEBUG_PREFIX} STATISTICS IMPORT: Domain '{DOMAIN_RECORDER}' not found in hass.services.async_services().")
                    else: 
                        # Build hourly deltas from 15-minute intervals
                        hourly_aggregated_data = {} 
                        for interval_data in processed_data.get("detailed_intervals", []):
                            try:
                                api_timestamp_utc_str = interval_data.get('timestamp_utc')
                                interval_end_utc = dt_module.fromisoformat(api_timestamp_utc_str.replace('Z', '+00:00'))
                                hour_bucket_start_utc = (interval_end_utc - timedelta(minutes=1)).replace(minute=0, second=0, microsecond=0)

                                interval_kwh_raw = interval_data.get('value')
                                if not isinstance(interval_kwh_raw, (int, float)):
                                    _LOGGER.warning(f"{DEBUG_PREFIX} STATISTICS IMPORT: Invalid value type for interval data {interval_data}, skipping.")
                                    continue
                                
                                interval_kwh = float(interval_kwh_raw)

                                # Safety: for consumption ensure non-negative
                                if self._api_profile == PROFILE_CONSUMPTION_ENERGY and interval_kwh < 0:
                                    _LOGGER.error(f"{DEBUG_PREFIX} STATISTICS IMPORT CRITICAL: Negative kWh value ({interval_kwh}) from API for CONSUMPTION profile {self.name} at {interval_end_utc.isoformat()}. Using 0 for delta.")
                                    interval_kwh = 0.0 

                                hourly_aggregated_data.setdefault(hour_bucket_start_utc, 0.0)
                                hourly_aggregated_data[hour_bucket_start_utc] += interval_kwh
                            except Exception as e_agg:
                                _LOGGER.warning(f"{DEBUG_PREFIX} STATISTICS IMPORT: Error during initial hourly aggregation {interval_data}: {e_agg}")
                        
                        _LOGGER.info(f"{DEBUG_PREFIX} STATISTICS IMPORT: Initial hourly aggregated data (deltas) for {self.entity_id}: {hourly_aggregated_data}")
                        
                        # --- 1️⃣ Zjisti poslední kumulativní hodnotu z databáze ---
                        cumulative_sum = 0.0
                        last_start = None
                        try:
                            # We're already running inside an executor thread (self._perform_update), so call the blocking
                            # get_last_statistics() directly — this is safe here and avoids any asyncio loop hustle.
                            last_stats = get_last_statistics(self.hass, 1, self.entity_id, True, {"sum","start_ts"})
                            if last_stats and self.entity_id in last_stats:
                                last_record = last_stats[self.entity_id][0]
                                last_start_dt = dt_module.fromtimestamp(last_record.get("start_ts"), tz=timezone.utc) if last_record.get("start_ts") else None
                                cumulative_sum = float(last_record.get("sum", 0.0) or 0.0)
                                _LOGGER.info(f"{DEBUG_PREFIX} Found previous cumulative sum={cumulative_sum} for {self.entity_id}")
                            else:
                                _LOGGER.info(f"{DEBUG_PREFIX} No previous statistics found for {self.entity_id}, starting from 0.0")
                        except Exception as e:
                            _LOGGER.warning(f"{DEBUG_PREFIX} Could not read previous statistics: {e}")
    
                        # Determine local-day UTC start and end for the target local date
                        local_tz = dateutil_tz.gettz(TIMEZONE_PRAGUE) or dt_util.now().tzinfo
                        day_start_local = datetime.datetime.combine(target_date_local, datetime.time.min, tzinfo=local_tz)
                        day_start_utc = day_start_local.astimezone(dateutil_tz.tzutc())
                        # Build list of hourly buckets for the local day expressed in UTC
                        hours_for_day = [day_start_utc + timedelta(hours=h) for h in range(0, 24)]
                        
                        stats_payload_list = []
                        daily_sum = 0.0
                        
                        # --- NEW: determine baseline (first value of the day) ---
                        non_zero_values = [v for v in hourly_aggregated_data.values() if isinstance(v, (int, float)) and v != 0]
                        baseline_offset = min(non_zero_values) if non_zero_values else 0.0
                        if baseline_offset != 0.0:
                            _LOGGER.debug(f"{DEBUG_PREFIX} STATISTICS IMPORT: Baseline offset detected for {self.entity_id}: {baseline_offset:.3f} kWh – adjusting to start from zero.")
                        
                        for hour_start_dt in hours_for_day:
                            delta = max(0.0, hourly_aggregated_data.get(hour_start_dt, 0.0) - baseline_offset)
                            daily_sum += delta
                            if last_start == None or last_start < hour_start_dt:
                                cumulative_sum += delta

                            daily_sum = round(daily_sum, 3)
                            cumulative_sum = round(cumulative_sum, 3)

                            stats_payload_list.append({
                                "start": hour_start_dt.isoformat(),
                                "state": float(daily_sum),   # denní přírůstek
                                "sum": float(cumulative_sum), # celkový kumulativní
                            })
                            _LOGGER.warning(
                                f"{DEBUG_PREFIX} WRITE hourly: start={hour_start_dt.isoformat()} "
                                f"state(daily)={daily_sum} sum(accumulated)={cumulative_sum}"
                            )


                        # --- 4️⃣ Uzávěrka dne ---
                        midnight_next_utc = (day_start_utc + timedelta(days=1))
                        stats_payload_list.append({
                            "start": midnight_next_utc.isoformat(),
                            "state": float(daily_sum),
                            "sum": float(cumulative_sum),
                        })
                        _LOGGER.warning(
                            f"{DEBUG_PREFIX} WRITE final: start={midnight_next_utc.isoformat()} "
                            f"state(daily)={daily_sum} sum(accumulated)={cumulative_sum}"
                        )

                        # Final filter: exclude any stats where max is None OR not numeric
                        cleaned_stats = []
                        skipped_invalid = 0
                        for s in stats_payload_list:
                            #1111 if s.get("max") is None or not isinstance(s.get("max"), (int, float)):
                            if s.get("sum") is None or not isinstance(s.get("sum"), (int, float)):
                                skipped_invalid += 1
                                continue
                            cleaned_stats.append(s)

                        _LOGGER.info(f"{DEBUG_PREFIX} STATISTICS IMPORT: Prepared {len(cleaned_stats)} hourly cumulative records for {self.entity_id} (skipped {skipped_invalid} invalid). Reset at local midnight (UTC): {day_start_utc.isoformat()}. Total sum: {cumulative_sum} kWh")
                        
                        if cleaned_stats:
                            service_call_payload = {
                                "statistic_id": self.entity_id,
                                "source": "recorder",
                                "has_mean": False,
                                "has_sum": True,
                                "stats": cleaned_stats,
                            }
                            if hasattr(self, '_attr_name') and self._attr_name:
                                service_call_payload["name"] = self._attr_name
                            if self.unit_of_measurement:
                                service_call_payload["unit_of_measurement"] = self.unit_of_measurement

                            _LOGGER.info(f"{DEBUG_PREFIX} STATISTICS IMPORT: Calling {DOMAIN_RECORDER}.import_statistics for {self.entity_id}. Payload sample (first 3): {str(service_call_payload['stats'][:3])} ...")
                            # schedule the service call on the event loop
                            service_coro = self.hass.services.async_call(DOMAIN_RECORDER, "import_statistics", service_call_payload)
                            self.hass.loop.call_soon_threadsafe(
                               self.hass.async_create_task,
                               service_coro
                            )
                            _LOGGER.info(f"{DEBUG_PREFIX} STATISTICS IMPORT: Service call to {DOMAIN_RECORDER}.import_statistics scheduled for {self.entity_id}.")
                        else:
                            _LOGGER.info(f"{DEBUG_PREFIX} STATISTICS IMPORT: No valid hourly statistics to import for {self.entity_id} for date {target_date_local.strftime('%Y-%m-%d')}.")
                elif not self.entity_id and self._target_unit == UnitOfEnergy.KILO_WATT_HOUR and processed_data.get("detailed_intervals"):
                    _LOGGER.warning(f"{DEBUG_PREFIX} Cannot import statistics for {self.name}: entity_id is still not available in _perform_update.")

                # mark this date as processed
                self.hass.data[DOMAIN][last_key] = target_date_local
                _LOGGER.info(
                    f"{DEBUG_PREFIX} Successfully imported data for day {target_date_local}, next run will start with the following day."
                )
                break 
            else:
                _LOGGER.info(f"{DEBUG_PREFIX} No data found for '{self.name}' for date {target_date_local.strftime('%Y-%m-%d')}. Status: {processed_data.get('attributes_to_set', {}).get(ATTR_LAST_UPDATE_STATUS)}")
                self._attributes[ATTR_LAST_UPDATE_STATUS] = processed_data["attributes_to_set"].get(ATTR_LAST_UPDATE_STATUS, "Fetch Failed")

        # if no state set -> provide informative last update status
        if not self._state:
            self._attributes[ATTR_LAST_UPDATE_STATUS] = f"No complete day data in {self._max_fetch_days}-day window"


    def _get_access_token_from_api(self) -> str | None:
        _LOGGER.info(f"{DEBUG_PREFIX} Requesting access token for '{self.name}' (EAN: {self._ean})")
        try:
            response = self._session.post(
                TOKEN_URL,
                data={'grant_type': API_GRANT_TYPE, 'client_id': self._client_id, 'client_secret': self._client_secret, 'scope': API_SCOPE},
                timeout=20
            )
            _LOGGER.debug(f"{DEBUG_PREFIX} Token API response status for '{self.name}': {response.status_code}")
            response.raise_for_status()
            token_data = response.json()
            token = token_data.get('access_token')
            if token:
                _LOGGER.info(f"{DEBUG_PREFIX} Access token successfully retrieved for '{self.name}'.")
                return token
            else:
                _LOGGER.error(f"{DEBUG_PREFIX} Access token not found in API response for '{self.name}'. Response: {token_data}")
                return None
        except requests.exceptions.HTTPError as e:
            _LOGGER.error(f"{DEBUG_PREFIX} HTTP error retrieving access token for '{self.name}': {e.response.status_code} - {e.response.text}")
        except requests.exceptions.RequestException as e:
            _LOGGER.error(f"{DEBUG_PREFIX} Network error retrieving access token for '{self.name}': {e}")
        except ValueError as e:
            _LOGGER.error(f"{DEBUG_PREFIX} JSON decode error retrieving access token for '{self.name}': {e}")
        return None

    def _fetch_and_process_data_for_day(self, token: str, period_start_local: dt_module, period_end_local: dt_module, target_date_local_for_log: dt_module) -> dict:
        _LOGGER.info(f"{DEBUG_PREFIX} _fetch_and_process_data_for_day CALLED for '{self.name}', date {target_date_local_for_log.strftime('%Y-%m-%d')}")
        from_utc = period_start_local.astimezone(dateutil_tz.tzutc())
        to_utc = period_end_local.astimezone(dateutil_tz.tzutc())

        api_params = {
            'ean': self._ean, 'profile': self._api_profile,
            'from': from_utc.strftime('%Y-%m-%dT%H:%M:%S.000Z'),
            'to': to_utc.strftime('%Y-%m-%dT%H:%M:%S.000Z'),
            'pageSize': API_PAGE_SIZE
        }
        _LOGGER.info(f"{DEBUG_PREFIX} Fetching data for '{self.name}' for date {target_date_local_for_log.strftime('%Y-%m-%d')} (Profile: {self._api_profile})")
        _LOGGER.debug(f"{DEBUG_PREFIX} API Request Params: {api_params}")

        result = {"data_found": False, "total_kwh_for_day": 0.0, "detailed_intervals": [], "attributes_to_set": {}}

        try:
            response = self._session.get(DATA_URL, headers={'Authorization': f'Bearer {token}'}, params=api_params, timeout=30)
            _LOGGER.debug(f"{DEBUG_PREFIX} Data API response status for '{self.name}' (date: {target_date_local_for_log.strftime('%Y-%m-%d')}): {response.status_code}")
            response.raise_for_status()
            api_response_data = response.json()

            detailed_intervals_temp = []
            total_kwh_for_day_temp = 0.0

            if api_response_data and isinstance(api_response_data, list) and len(api_response_data) > 0 and \
               'data' in api_response_data[0] and isinstance(api_response_data[0]['data'], list) and \
               len(api_response_data[0]['data']) > 0:
                raw_intervals_data = api_response_data[0]['data']
                api_units = api_response_data[0].get("units", "KW").upper()
                _LOGGER.info(f"{DEBUG_PREFIX} Received {len(raw_intervals_data)} raw interval(s) from API for '{self.name}'. API units: {api_units}")

                for item_idx, item in enumerate(raw_intervals_data):
                    timestamp_str = item.get('timestamp')
                    value_from_api = item.get('value')
                    status_code = item.get('status')

                    if isinstance(value_from_api, (int, float)) and timestamp_str:
                        value_interval = float(value_from_api)
                        if self._is_value_direct_kwh: 
                            if self._api_profile == PROFILE_CONSUMPTION_ENERGY and value_interval < 0:
                                _LOGGER.error(f"{DEBUG_PREFIX} API returned negative value {value_interval} for CONSUMPTION profile {self._api_profile} at {timestamp_str}. Using 0.")
                                value_interval = 0.0
                            total_kwh_for_day_temp += value_interval
                        
                        detailed_intervals_temp.append({
                            'timestamp_utc': timestamp_str, 'value': round(value_interval, 3),
                            'unit_of_value': api_units, 'status': status_code
                        })
                    else:
                        _LOGGER.warning(f"{DEBUG_PREFIX} Skipping invalid interval item [{item_idx}]: {item} for '{self.name}'")
                
                if detailed_intervals_temp:
                    result["data_found"] = True
                    result["total_kwh_for_day"] = round(total_kwh_for_day_temp, 3) if self._is_value_direct_kwh else 0.0
                    result["detailed_intervals"] = detailed_intervals_temp
                    result["attributes_to_set"] = {
                        ATTR_LAST_UPDATE_STATUS: 'Success', ATTR_FIFTEEN_MINUTE_DATA: detailed_intervals_temp,
                        ATTR_API_DATA_POINTS_RECEIVED: len(detailed_intervals_temp),
                        ATTR_DATA_FETCHED_FOR_DATE_LOCAL: target_date_local_for_log.strftime('%Y-%m-%d'),
                        ATTR_DATA_RANGE_UTC_FROM: from_utc.isoformat(), ATTR_DATA_RANGE_UTC_TO: to_utc.isoformat(),
                        ATTR_LAST_SUCCESSFUL_FETCH_UTC: dt_util.utcnow().isoformat()
                    }
                    _LOGGER.info(f"{DEBUG_PREFIX} Data processed for '{self.name}'. Total kWh (if energy sensor): {result['total_kwh_for_day']}")
                elif not detailed_intervals_temp and raw_intervals_data:
                    _LOGGER.warning(f"{DEBUG_PREFIX} No valid intervals processed from API data for '{self.name}', though raw data was present.")
                    result["attributes_to_set"][ATTR_LAST_UPDATE_STATUS] = 'No valid intervals in data'
            elif api_response_data and 'error' in api_response_data and api_response_data['error'] == 'No results':
                _LOGGER.info(f"{DEBUG_PREFIX} API returned 'No results' for '{self.name}'.")
                result["attributes_to_set"][ATTR_LAST_UPDATE_STATUS] = 'Success (No results from API)'
            elif api_response_data and isinstance(api_response_data, list) and len(api_response_data) > 0 and \
                 'data' in api_response_data[0] and isinstance(api_response_data[0]['data'], list) and \
                 len(api_response_data[0]['data']) == 0:
                _LOGGER.info(f"{DEBUG_PREFIX} API returned an empty 'data' list for '{self.name}'.")
                result["attributes_to_set"][ATTR_LAST_UPDATE_STATUS] = 'Success (No data points)'
            else:
                _LOGGER.warning(f"{DEBUG_PREFIX} Unexpected API response format or no data for '{self.name}'. Response: {str(api_response_data)[:200]}")
                result["attributes_to_set"][ATTR_LAST_UPDATE_STATUS] = 'Unexpected API response'
        except requests.exceptions.HTTPError as e:
            _LOGGER.error(f"{DEBUG_PREFIX} API HTTP error for '{self.name}': {e.response.status_code} - {e.response.text[:200]}", exc_info=True)
            result["attributes_to_set"][ATTR_LAST_UPDATE_STATUS] = f'API HTTP Error {e.response.status_code}'
        except requests.exceptions.RequestException as e:
            _LOGGER.error(f"{DEBUG_PREFIX} API network error for '{self.name}': {e}", exc_info=True)
            result["attributes_to_set"][ATTR_LAST_UPDATE_STATUS] = 'API Network Error'
        except ValueError as e:
            responseText = response.text if 'response' in locals() and response else 'N/A'
            _LOGGER.error(f"{DEBUG_PREFIX} API JSON decode error for '{self.name}': {e}. Response text: {responseText[:200]}", exc_info=True)
            result["attributes_to_set"][ATTR_LAST_UPDATE_STATUS] = 'API JSON Decode Error'
        except Exception as e:
            _LOGGER.error(f"{DEBUG_PREFIX} Generic error processing data for '{self.name}': {e}", exc_info=True)
            result["attributes_to_set"][ATTR_LAST_UPDATE_STATUS] = 'Data Processing Error'
        
        if not result["data_found"] and ATTR_LAST_UPDATE_STATUS not in result["attributes_to_set"]:
             result["attributes_to_set"][ATTR_LAST_UPDATE_STATUS] = f'Failed fetch for {target_date_local_for_log.strftime("%Y-%m-%d")}'
        _LOGGER.info(f"{DEBUG_PREFIX} _fetch_and_process_data_for_day FINISHED for '{self.name}'. Data found: {result['data_found']}")
        return result


class EGDPowerConsumptionSensor(EGDBaseSensor):
    """Sensor for EGD power consumption (ICC1 - kW)."""
    def __init__(self, hass: HomeAssistant, client_id: str, client_secret: str, ean: str, days_offset: int, max_fetch_days: int, entry_id: str):
        _LOGGER.info(f"{DEBUG_PREFIX} EGDPowerConsumptionSensor __init__ for EAN {ean}")
        super().__init__(hass, client_id, client_secret, ean, days_offset, max_fetch_days,
                         PROFILE_CONSUMPTION_POWER, f"Consumption Power ({PROFILE_CONSUMPTION_POWER})", entry_id,
                         is_value_direct_kwh=False,
                         target_unit=UnitOfPower.KILO_WATT,
                         target_state_class=SensorStateClass.MEASUREMENT)

class EGDPowerProductionSensor(EGDBaseSensor):
    """Sensor for EGD power production (ISC1 - kW)."""
    def __init__(self, hass: HomeAssistant, client_id: str, client_secret: str, ean: str, days_offset: int, max_fetch_days: int, entry_id: str):
        _LOGGER.info(f"{DEBUG_PREFIX} EGDPowerProductionSensor __init__ for EAN {ean}")
        super().__init__(hass, client_id, client_secret, ean, days_offset, max_fetch_days,
                         PROFILE_PRODUCTION_POWER, f"Production Power ({PROFILE_PRODUCTION_POWER})", entry_id,
                         is_value_direct_kwh=False,
                         target_unit=UnitOfPower.KILO_WATT,
                         target_state_class=SensorStateClass.MEASUREMENT)

class EGDEnergyConsumptionSensor(EGDBaseSensor):
    """Sensor for EGD energy consumption (ICQ2 - kWh)."""
    def __init__(self, hass: HomeAssistant, client_id: str, client_secret: str, ean: str, days_offset: int, max_fetch_days: int, entry_id: str):
        _LOGGER.info(f"{DEBUG_PREFIX} EGDEnergyConsumptionSensor __init__ for EAN {ean}")
        super().__init__(hass, client_id, client_secret, ean, days_offset, max_fetch_days,
                         PROFILE_CONSUMPTION_ENERGY, f"Consumption Energy ({PROFILE_CONSUMPTION_ENERGY})", entry_id,
                         is_value_direct_kwh=True,
                         target_unit=UnitOfEnergy.KILO_WATT_HOUR,
                         target_state_class=SensorStateClass.TOTAL_INCREASING
                         )

class EGDEnergyProductionSensor(EGDBaseSensor):
    """Sensor for EGD energy production (ISQ2 - kWh)."""
    def __init__(self, hass: HomeAssistant, client_id: str, client_secret: str, ean: str, days_offset: int, max_fetch_days: int, entry_id: str):
        _LOGGER.info(f"{DEBUG_PREFIX} EGDEnergyProductionSensor __init__ for EAN {ean}")
        super().__init__(hass, client_id, client_secret, ean, days_offset, max_fetch_days,
                         PROFILE_PRODUCTION_ENERGY, f"Production Energy ({PROFILE_PRODUCTION_ENERGY})", entry_id,
                         is_value_direct_kwh=True,
                         target_unit=UnitOfEnergy.KILO_WATT_HOUR,
                         target_state_class=SensorStateClass.TOTAL_INCREASING
                         )


class EGDStatusSensor(SensorEntity):
    """Sensor to reflect the status of EGD data retrieval."""

    def __init__(self, hass: HomeAssistant, ean: str, days_offset_config: int, consumption_sensor_unique_id: str, entry_id: str):
        self.hass = hass
        self._ean = str(ean)
        self._days_offset_config = days_offset_config
        self._consumption_sensor_unique_id = consumption_sensor_unique_id
        self._entry_id = entry_id

        self._attr_name = f"EGD Data Status {self._ean} (offset {self._days_offset_config}d)"
        self._attr_unique_id = f"{DOMAIN}_status_{self._ean}_{self._days_offset_config}"
        self._state: str | None = "Initializing"
        self._attributes: dict[str, any] = {}
        _LOGGER.info(f"{DEBUG_PREFIX} EGDStatusSensor __init__ CALLED (v{SCRIPT_VERSION}): {self.name}, watching {self._consumption_sensor_unique_id}")

    @property
    def device_info(self):
        try:
            return {
                "identifiers": {(DOMAIN, str(self._ean))},
                "name": f"EGD Device ({str(self._ean)})",
                "manufacturer": "EGD CZ",
            }
        except Exception as e:
            _LOGGER.error(f"{DEBUG_PREFIX} Error generating device_info for StatusSensor {self.name}: {e}", exc_info=True)
            return None

    @property
    def state(self) -> str | None:
        return self._state

    @property
    def extra_state_attributes(self) -> dict[str, any]:
        return self._attributes

    @property
    def icon(self) -> str:
        current_state_lower = str(self.state).lower() if self.state else "unknown"
        if "success" in current_state_lower or "ok" in current_state_lower :
            return "mdi:check-circle-outline"
        elif "error" in current_state_lower or "failed" in current_state_lower or "not_found" in current_state_lower or "not_registered" in current_state_lower:
            return "mdi:alert-circle-outline"
        return "mdi:timer-sand"

    async def async_update(self) -> None:
        _LOGGER.debug(f"{DEBUG_PREFIX} Updating EGDStatusSensor for '{self.name}'")
        if not self.hass:
            _LOGGER.error(f"{DEBUG_PREFIX} StatusSensor: HASS object not available in async_update for {self.name}.")
            return

        entity_reg = er.async_get(self.hass)
        consumption_entity_id = entity_reg.async_get_entity_id("sensor", DOMAIN, self._consumption_sensor_unique_id)
        _LOGGER.debug(f"{DEBUG_PREFIX} StatusSensor '{self.name}': Attempting to find watched sensor with unique_id '{self._consumption_sensor_unique_id}'. Found entity_id: {consumption_entity_id}")


        if not consumption_entity_id:
            _LOGGER.warning(f"{DEBUG_PREFIX} StatusSensor '{self.name}': Watched consumption sensor with unique_id '{self._consumption_sensor_unique_id}' not found in entity registry. This is the problematic warning.")
            self._state = "Error: Main sensor not registered"
            self._attributes[ATTR_LOOKUP_STATUS] = f"Consumption sensor unique_id '{self._consumption_sensor_unique_id}' not in registry."
            return

        consumption_state_obj = self.hass.states.get(consumption_entity_id)
        if not consumption_state_obj:
            _LOGGER.warning(f"{DEBUG_PREFIX} StatusSensor '{self.name}': State object for consumption sensor '{consumption_entity_id}' (unique_id: {self._consumption_sensor_unique_id}) not found.")
            self._state = "Pending: Main sensor state unavailable"
            self._attributes[ATTR_LOOKUP_STATUS] = f"State for '{consumption_entity_id}' unavailable."
            return
        
        _LOGGER.debug(f"{DEBUG_PREFIX} StatusSensor '{self.name}': Successfully retrieved state object for watched sensor '{consumption_entity_id}'.")

        self._attributes[ATTR_WATCHED_CONSUMER_SENSOR_ENTITY_ID] = consumption_entity_id
        last_status = consumption_state_obj.attributes.get(ATTR_LAST_UPDATE_STATUS, 'N/A')
        self._attributes[ATTR_CONSUMER_LAST_UPDATE_STATUS] = last_status
        self._attributes[ATTR_CONSUMER_LAST_SUCCESSFUL_FETCH_UTC] = consumption_state_obj.attributes.get(ATTR_LAST_SUCCESSFUL_FETCH_UTC)
        self._attributes[ATTR_CONSUMER_DATA_FETCHED_FOR_DATE_LOCAL] = consumption_state_obj.attributes.get(ATTR_DATA_FETCHED_FOR_DATE_LOCAL)
        self._attributes["watched_sensor_state"] = consumption_state_obj.state
        self._attributes["watched_sensor_unit"] = consumption_state_obj.attributes.get("unit_of_measurement")

        if str(last_status).startswith('Success'):
            self._state = f"OK ({last_status})"
        elif last_status == 'N/A':
            self._state = "Pending first data"
        else:
            self._state = f"Error ({last_status})"
        _LOGGER.debug(f"{DEBUG_PREFIX} StatusSensor '{self.name}' updated. State: {self.state}, Consumer Last Status: {last_status}")
