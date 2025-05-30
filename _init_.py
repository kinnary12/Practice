import logging
import azure.functions as func
import pandas as pd
import io
import os
from datetime import datetime
from azure.digitaltwins.core import DigitalTwinsClient
from azure.identity import DefaultAzureCredential
from azure.core.exceptions import HttpResponseError

# Configure logging for structured context
def get_logger():
    logger = logging.getLogger("FurnaceBlobProcessor")
    if not logger.handlers:
        handler = logging.StreamHandler()
        formatter = logging.Formatter(
            "[%(asctime)s] %(levelname)s %(name)s %(message)s"
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)
    logger.setLevel(logging.INFO)
    return logger

logger = get_logger()

# Constants and mappings
SOURCE_TO_ENUM = {
    "Alloy Addition": "AlloyAddition",
    "Aluminium Charging": "AluminiumCharging",
    "Sampling": "Sampling",
    "Scrap Charging": "ScrapCharging",
    "Skimming": "Skimming",
    "Stirring": "Stirring",
    "Unknown": "Unknown"
}

REQUIRED_COLUMNS = [
    "fce_door_closed", "fce_door_open_reason", "t_stamp",
    "fce_bath_temp_cv", "fce_bath_temp_pv", "fce_bath_temp_sp",
    "fce_roof_temp_cv", "fce_roof_temp_pv", "fce_roof_temp_sp",
    "fce_gas_flow_burner_pv", "fce_exhaust_temp_pv",
    "fce_pressure_pv", "fce_oxygen_pv", "casting_status"
]

def map_to_enum(source_value: str) -> str:
    if source_value is None:
        return "Unknown"
    return SOURCE_TO_ENUM.get(str(source_value).strip(), "Unknown")

def map_fce_door_closed(value) -> str:
    try:
        if value is None:
            return "Unknown"
        int_val = int(value)
        if int_val == 0:
            return "CLOSE"
        elif int_val == 1:
            return "OPEN"
        else:
            return "Unknown"
    except Exception:
        return "Unknown"

class TwinUpdater:
    """Handles Digital Twins authentication and update logic. Reuses client per invocation."""
    def __init__(self, adt_url: str):
        if not adt_url:
            logger.error("ADT_SERVICE_URL not set")
            raise RuntimeError("ADT_SERVICE_URL not set")
        self.adt_url = adt_url
        self.credential = DefaultAzureCredential()
        self.client = DigitalTwinsClient(self.adt_url, self.credential)

    def update_twin(self, dt_id: str, patch: list, context: dict = None, retries=2):
        """Attempts to update a digital twin, with basic retries and detailed logging."""
        attempt = 0
        while attempt <= retries:
            try:
                self.client.update_digital_twin(dt_id, patch)
                logger.info(
                    f"Patched twin {dt_id}", extra={"patch": patch, **(context or {})}
                )
                return True
            except HttpResponseError as e:
                logger.error(
                    f"Failed to patch twin {dt_id} (try {attempt+1}/{retries+1}): {e}",
                    exc_info=True,
                    extra={"patch": patch, **(context or {})}
                )
                if attempt == retries:
                    return False
            except Exception as ex:
                logger.error(
                    f"Unexpected error updating twin {dt_id} (try {attempt+1}/{retries+1}): {ex}",
                    exc_info=True,
                    extra={"patch": patch, **(context or {})}
                )
                if attempt == retries:
                    return False
            attempt += 1

def validate_columns(df: pd.DataFrame, required_cols: list):
    missing = [col for col in required_cols if col not in df.columns]
    if missing:
        logger.warning(f"Missing columns in input DataFrame: {missing}")
    return not missing

def to_iso_timestamp(val):
    if pd.isnull(val):
        return None
    if isinstance(val, pd.Timestamp):
        return val.isoformat()
    elif isinstance(val, datetime):
        return val.isoformat()
    else:
        try:
            # Try to parse string to datetime
            dt = pd.to_datetime(val, utc=True)
            return dt.isoformat()
        except Exception:
            return str(val)

def update_digital_twin_state(twin_updater: TwinUpdater, dt_id: str, fce_door_closed: str, fce_door_open_reason: str):
    patch = [
        {"op": "replace", "path": "/hasOpenReason", "value": str(fce_door_open_reason)},
        {"op": "replace", "path": "/hasStateValue", "value": str(fce_door_closed)},
    ]
    twin_updater.update_twin(dt_id, patch, context={"twin_type": "state"})

def update_property_value(twin_updater: TwinUpdater, dt_id: str, t_stamp, value):
    if pd.isnull(value) or t_stamp is None:
        logger.debug(f"Skipping property update for {dt_id}: missing value or timestamp")
        return
    patch = [
        {"op": "replace", "path": "/hasTimestamp", "value": t_stamp},
        {"op": "replace", "path": "/hasValue", "value": float(value)}
    ]
    twin_updater.update_twin(dt_id, patch, context={"twin_type": "property"})

def update_condition_and_step(twin_updater: TwinUpdater, dt_id_condition, dt_id_step, achieved, t_stamp):
    patch_cond = [
        {"op": "replace", "path": "/conditionAchieved", "value": achieved},
        {"op": "replace", "path": "/hasEndTimestamp", "value": t_stamp},
        {"op": "replace", "path": "/hasStartTimestamp", "value": t_stamp}
    ]
    patch_step = [
        {"op": "replace", "path": "/isCurrentStep", "value": achieved},
        {"op": "replace", "path": "/hasEndTimestamp", "value": t_stamp},
        {"op": "replace", "path": "/hasStartTimestamp", "value": t_stamp}
    ]
    # Update each twin independently and log partial failures
    cond_ok = twin_updater.update_twin(dt_id_condition, patch_cond, context={"twin_type": "condition"})
    step_ok = twin_updater.update_twin(dt_id_step, patch_step, context={"twin_type": "step"})
    if not cond_ok or not step_ok:
        logger.error(f"Partial failure in updating condition ({dt_id_condition}) or step ({dt_id_step})")

def update_subprocess(twin_updater: TwinUpdater, dt_id_subprocess, achieved):
    patch = [
        {"op": "replace", "path": "/isCurrentSubProcess", "value": achieved}
    ]
    twin_updater.update_twin(dt_id_subprocess, patch, context={"twin_type": "subprocess"})

def process_blob(blob: func.InputStream, furnace_number: int, twin_updater: TwinUpdater) -> None:
    logger.info(f"New blob detected for furnace {furnace_number}: {blob.name}", extra={"furnace_number": furnace_number})
    try:
        data = blob.read()
        buffer = io.BytesIO(data)
        df = pd.read_parquet(buffer)
        logger.info(f"Read {len(df)} rows with columns: {list(df.columns)} from blob {blob.name}")

        if df.empty:
            logger.warning("No data found in the parquet file.", extra={"blob_name": blob.name})
            return

        if not validate_columns(df, REQUIRED_COLUMNS):
            logger.error("Required columns missing. Skipping blob.", extra={"blob_name": blob.name})
            return

        latest_row = df.iloc[-1]
        raw_fce_door_closed = latest_row.get("fce_door_closed")
        source_reason = latest_row.get("fce_door_open_reason")
        fce_door_closed = map_fce_door_closed(raw_fce_door_closed)
        fce_door_open_reason = map_to_enum(source_reason)
        dt_id_state = f"stateOpenCloseDoor_{furnace_number}"

        # Always use ISO 8601 UTC for timestamps
        t_stamp = to_iso_timestamp(latest_row.get("t_stamp"))
        if not t_stamp:
            logger.warning("Missing or invalid timestamp in blob row. Skipping update.", extra={"blob_name": blob.name})
            return

        update_digital_twin_state(twin_updater, dt_id_state, fce_door_closed, fce_door_open_reason)

        # All property value updates (CV, PV, SP, etc.)
        # Map of df field -> twin name template
        property_map = {
            "fce_bath_temp_cv": f"propertyValueHeatingPowerPercentBathTank_{furnace_number}",
            "fce_bath_temp_pv": f"propertyValueTemperatureBathTank_{furnace_number}",
            "fce_bath_temp_sp": f"propertyValueTemperatureBathThresholdTank_{furnace_number}",
            "fce_roof_temp_cv": f"propertyValueHeatingPowerPercentRoofTank_{furnace_number}",
            "fce_roof_temp_sp": f"propertyValueTemperatureRoofThresholdTank_{furnace_number}",
            "fce_exhaust_temp_pv": f"propertyValueTemperatureExhaustFurnace_{furnace_number}",
            "fce_pressure_pv": f"propertyValuePressureFurnace_{furnace_number}",
            "fce_oxygen_pv": f"propertyValueConcentrationAirBurner_{furnace_number}",
            "casting_status": f"propertyValueCastingStatusFurnace_{furnace_number}",
        }

        for field, twin_template in property_map.items():
            value = latest_row.get(field)
            update_property_value(twin_updater, twin_template, t_stamp, value)

        # Fields where twin name logic varies by furnace
        fce_roof_temp_pv = latest_row.get("fce_roof_temp_pv")
        if pd.notnull(fce_roof_temp_pv):
            dt_id_roof_pv = None
            if furnace_number == 10:
                dt_id_roof_pv = "propertyValueTemperatureRoofTank_10"
            elif furnace_number == 12:
                dt_id_roof_pv = "propertyValueHeatingPowerPercentRoofTank_12"
            if dt_id_roof_pv:
                update_property_value(twin_updater, dt_id_roof_pv, t_stamp, fce_roof_temp_pv)

        fce_gas_flow_burner_pv = latest_row.get("fce_gas_flow_burner_pv")
        if pd.notnull(fce_gas_flow_burner_pv):
            dt_id_gas_flow = None
            if furnace_number == 10:
                dt_id_gas_flow = "propertyValueFlowRateGasBurner_10"
            elif furnace_number == 12:
                dt_id_gas_flow = "propertyValueTemperatureRoofThresholdTank_12"
            if dt_id_gas_flow:
                update_property_value(twin_updater, dt_id_gas_flow, t_stamp, fce_gas_flow_burner_pv)

        # Standardize numerics for conditions (handle nan)
        casting_status = latest_row.get("casting_status")
        cs = float(casting_status) if pd.notnull(casting_status) else None
        fdc = int(raw_fce_door_closed) if pd.notnull(raw_fce_door_closed) else None
        fbtpv = float(latest_row.get("fce_bath_temp_pv")) if pd.notnull(latest_row.get("fce_bath_temp_pv")) else None
        fbtsp = float(latest_row.get("fce_bath_temp_sp")) if pd.notnull(latest_row.get("fce_bath_temp_sp")) else None
        fgfbpv = float(fce_gas_flow_burner_pv) if pd.notnull(fce_gas_flow_burner_pv) else None
        fdoor_reason = map_to_enum(source_reason)

        # 1. Preheating
        cond_1 = (
            (cs == 0) and
            (fdc == 1) and
            (fbtpv is not None and fbtsp is not None and fbtpv < fbtsp) and
            (fgfbpv is not None and fgfbpv > 0)
        )
        update_condition_and_step(
            twin_updater,
            dt_id_condition=f"conditionPreheating_{furnace_number}",
            dt_id_step=f"stepPreheating_{furnace_number}",
            achieved=cond_1,
            t_stamp=t_stamp,
        )

        # 2. Skimming
        cond_2 = (
            (cs == 0) and
            (fdc == 0) and
            (fdoor_reason == "Skimming")
        )
        update_condition_and_step(
            twin_updater,
            dt_id_condition=f"conditionSkimming_{furnace_number}",
            dt_id_step=f"stepSkimming_{furnace_number}",
            achieved=cond_2,
            t_stamp=t_stamp,
        )

        # SubProcess
        is_subprocess = cond_1 or cond_2
        update_subprocess(
            twin_updater=twin_updater,
            dt_id_subprocess=f"subProcessPreparation_{furnace_number}",
            achieved=is_subprocess
        )

        # 3. Casting
        cond_casting = (
            (cs is not None and cs > 0) and
            (fbtpv is not None and fbtpv < 1000)
        )
        update_condition_and_step(
            twin_updater,
            dt_id_condition=f"conditionCasting_{furnace_number}",
            dt_id_step=f"stepCastingStep_{furnace_number}",
            achieved=cond_casting,
            t_stamp=t_stamp
        )

        # 4. Holding
        cond_holding = (
            (cs == 0) and
            (fdc == 1) and
            (fbtpv is not None and fbtsp is not None and abs(fbtpv - fbtsp) < 10)
        )
        update_condition_and_step(
            twin_updater,
            dt_id_condition=f"conditionHolding_{furnace_number}",
            dt_id_step=f"stepHolding_{furnace_number}",
            achieved=cond_holding,
            t_stamp=t_stamp
        )

        # 5. Loading Scrap Adjustment
        cond_loading_scrap_adjustment = (
            (cs == 1) and
            (fdc == 0) and
            (fdoor_reason == "ScrapCharging")
        )
        update_condition_and_step(
            twin_updater,
            dt_id_condition=f"conditionLoadingScrapAdjustment_{furnace_number}",
            dt_id_step=f"stepLoadingScrapAdjustment_{furnace_number}",
            achieved=cond_loading_scrap_adjustment,
            t_stamp=t_stamp
        )

        # 6. Loading Scrap
        cond_loading_scrap = (
            (cs == 0) and
            (fdc == 0) and
            (fdoor_reason == "ScrapCharging")
        )
        update_condition_and_step(
            twin_updater,
            dt_id_condition=f"conditionLoadingScrap_{furnace_number}",
            dt_id_step=f"stepLoadingScrap_{furnace_number}",
            achieved=cond_loading_scrap,
            t_stamp=t_stamp
        )

        # 7. Pouring Aluminium Adjustment
        cond_pouring_al_adj = (
            (cs == 1) and
            (fdc == 0) and
            (fdoor_reason == "Sampling")
        )
        update_condition_and_step(
            twin_updater,
            dt_id_condition=f"conditionPouringAluminiumAdjustment_{furnace_number}",
            dt_id_step=f"stepPouringAluminiumAdjustment_{furnace_number}",
            achieved=cond_pouring_al_adj,
            t_stamp=t_stamp
        )

        # 8. Pouring Aluminium
        cond_pouring_al = (
            (fdc == 0) and
            (fdoor_reason == "AluminiumCharging")
        )
        update_condition_and_step(
            twin_updater,
            dt_id_condition=f"conditionPouringAluminium_{furnace_number}",
            dt_id_step=f"stepPouringAluminium_{furnace_number}",
            achieved=cond_pouring_al,
            t_stamp=t_stamp
        )

        # 9. Pouring Metals Adjustment
        cond_pouring_metals_adj = (
            (cs == 1) and
            (fdc == 0) and
            (fdoor_reason == "AlloyAddition")
        )
        update_condition_and_step(
            twin_updater,
            dt_id_condition=f"conditionPouringMetalsAdjustment_{furnace_number}",
            dt_id_step=f"stepPouringMetalsAdjustment_{furnace_number}",
            achieved=cond_pouring_metals_adj,
            t_stamp=t_stamp
        )

        # 10. Pouring Metals
        cond_pouring_metals = (
            (cs == 0) and
            (fdc == 0) and
            (fdoor_reason == "AlloyAddition")
        )
        update_condition_and_step(
            twin_updater,
            dt_id_condition=f"conditionPouringMetals_{furnace_number}",
            dt_id_step=f"stepPouringMetals_{furnace_number}",
            achieved=cond_pouring_metals,
            t_stamp=t_stamp
        )

        # 11. Sampling
        cond_sampling = (
            (cs == 0) and
            (fdc == 0) and
            (fdoor_reason == "Sampling")
        )
        update_condition_and_step(
            twin_updater,
            dt_id_condition=f"conditionSampling_{furnace_number}",
            dt_id_step=f"stepSampling_{furnace_number}",
            achieved=cond_sampling,
            t_stamp=t_stamp
        )

        # 12. Stirring
        cond_stirring = (
            (cs == 0) and
            (fdc == 0) and
            (fdoor_reason == "Stirring")
        )
        update_condition_and_step(
            twin_updater,
            dt_id_condition=f"conditionStirring_{furnace_number}",
            dt_id_step=f"stepStirring_{furnace_number}",
            achieved=cond_stirring,
            t_stamp=t_stamp
        )

    except Exception as e:
        logger.error(f"Error processing blob {blob.name}: {e}", exc_info=True, extra={"blob_name": blob.name})

def main_fur10(blob: func.InputStream) -> None:
    adt_url = os.environ.get("ADT_SERVICE_URL")
    twin_updater = TwinUpdater(adt_url)
    process_blob(blob, 10, twin_updater)

def main_fur12(blob: func.InputStream) -> None:
    adt_url = os.environ.get("ADT_SERVICE_URL")
    twin_updater = TwinUpdater(adt_url)
    process_blob(blob, 12, twin_updater)
