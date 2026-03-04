# File: SubjectiveTickerDataSource.py

import os
import time
import threading
from datetime import datetime
from subjective_abstract_data_source_package import SubjectiveRealTimeDataSource
from brainboost_data_source_logger_package.BBLogger import BBLogger


UNIT_TO_SECONDS = {
    "ms": 0.001,
    "sec": 1.0,
    "min": 60.0,
    "hour": 3600.0,
}


class SubjectiveTickerDataSource(SubjectiveRealTimeDataSource):
    """
    A real-time data source that triggers ticks at configurable intervals.

    This data source emits "tick" events that can be used to trigger downstream
    data sources in a pipeline. It supports two timing modes:

    1. Time + Unit: tick every N units (hour, min, sec, ms)
    2. Cron expression: tick according to a cron schedule

    Provide either (time + unit) OR cron, not both.

    Each tick emits:
    - A notification to all subscriber data sources
    - A Redis signal on the configured channel

    Parameters (via params dict):
        time (float): Numeric interval value (e.g., 30). Used with 'unit'.
        unit (str): One of "hour", "min", "sec", "ms". Used with 'time'.
        cron (str): Cron expression string (e.g., "*/5 * * * *"). Alternative to time+unit.
        subscriber (str): Name of an existing connection to subscribe to this ticker.
        redis_channel (str): Redis channel for tick notifications. Default: "ticker_events"
        datasource_id (str): Identifier for this ticker instance

    Example usage:
        ```python
        # Tick every 30 seconds
        ticker = SubjectiveTickerDataSource(
            name="my_ticker",
            params={
                "time": 30,
                "unit": "sec",
                "redis_channel": "pipeline_triggers"
            }
        )

        # Tick every 5 minutes using cron
        ticker = SubjectiveTickerDataSource(
            name="cron_ticker",
            params={
                "cron": "*/5 * * * *"
            }
        )
        ```
    """

    connection_type = "Ticker"
    connection_fields = [
        "time",
        "unit",
        "cron",
        "subscriber"
    ]

    def __init__(self, name=None, session=None, dependency_data_sources=None,
                 subscribers=None, params=None):
        super().__init__(
            name=name,
            session=session,
            dependency_data_sources=dependency_data_sources or [],
            subscribers=subscribers,
            params=params or {}
        )

        # Timing configuration: either cron or time+unit
        self._cron_expression = self.params.get("cron", "").strip() if self.params.get("cron") else ""
        self._time_value = self._coerce_float_param("time", 0)
        self._unit = self.params.get("unit", "sec").lower()

        if self._cron_expression:
            self._mode = "cron"
        else:
            self._mode = "interval"
            if self._unit not in UNIT_TO_SECONDS:
                BBLogger.log(f"Unknown unit '{self._unit}', defaulting to 'sec'")
                self._unit = "sec"
            if self._time_value <= 0:
                BBLogger.log("Time value must be > 0, defaulting to 60")
                self._time_value = 60

        # Subscriber configuration
        self._subscriber_connection = self.params.get("subscriber", "")

        # Redis configuration
        self._redis_channel = self.params.get("redis_channel", "ticker_events")
        self._datasource_id = self.params.get("datasource_id", name or "ticker")

        # Runtime state
        self._tick_number = 0
        self._start_time = None
        self._stop_event = threading.Event()
        self._cron_iter = None

        if self._mode == "cron":
            BBLogger.log(
                f"SubjectiveTickerDataSource initialized: cron={self._cron_expression}"
            )
        else:
            BBLogger.log(
                f"SubjectiveTickerDataSource initialized: "
                f"time={self._time_value}, unit={self._unit}"
            )

    def _coerce_bool_param(self, key, default):
        """Convert parameter to boolean."""
        try:
            value = self.params.get(key, default)
            if isinstance(value, bool):
                return value
            if isinstance(value, str):
                return value.lower() in ('true', '1', 'yes', 'on')
            return bool(value)
        except (TypeError, ValueError):
            return default

    def _get_interval_seconds(self):
        """
        Calculate the interval in seconds based on time and unit.

        Returns:
            float: Interval in seconds, or None if using cron mode.
        """
        if self._mode == "cron":
            return None
        multiplier = UNIT_TO_SECONDS.get(self._unit, 1.0)
        return self._time_value * multiplier

    def _get_next_cron_delay(self):
        """
        Get the delay until the next cron trigger.

        Returns:
            float: Seconds until next cron trigger
        """
        if self._cron_iter is None:
            return 60.0  # Fallback

        try:
            next_time = self._cron_iter.get_next(datetime)
            now = datetime.now()
            delay = (next_time - now).total_seconds()
            return max(0.1, delay)  # Minimum 0.1 second delay
        except Exception as e:
            BBLogger.log(f"Error calculating next cron time: {e}")
            return 60.0

    def _initialize_monitoring(self):
        """Initialize tick monitoring resources."""
        self._tick_number = 0
        self._start_time = time.time()
        self._stop_event.clear()

        # Initialize cron iterator if using cron mode
        if self._mode == "cron":
            try:
                from croniter import croniter
                self._cron_iter = croniter(self._cron_expression, datetime.now())
                BBLogger.log(f"Cron expression initialized: {self._cron_expression}")
            except ImportError:
                BBLogger.log("croniter not installed, falling back to 60 sec interval")
                self._mode = "interval"
                self._time_value = 60
                self._unit = "sec"
            except Exception as e:
                BBLogger.log(f"Invalid cron expression '{self._cron_expression}': {e}")
                self._mode = "interval"
                self._time_value = 60
                self._unit = "sec"

        BBLogger.log(f"Tick monitoring initialized for {self.__class__.__name__}")

    def _start_monitoring_implementation(self):
        """
        Main tick loop that emits notifications at configured intervals.

        This runs in a background thread and sends tick notifications to all
        subscribers and via Redis at each tick.
        """
        if self._mode == "cron":
            interval_desc = f"cron({self._cron_expression})"
        else:
            interval_desc = f"{self._time_value} {self._unit}"
        BBLogger.log(f"Starting tick loop: {interval_desc}")

        while self._monitoring_active and not self._stop_event.is_set():
            # Calculate wait time based on mode
            if self._mode == "cron":
                wait_time = self._get_next_cron_delay()
            else:
                wait_time = self._get_interval_seconds()

            # Wait for next tick interval (using Event.wait for interruptible sleep)
            if self._stop_event.wait(timeout=wait_time):
                # Stop event was set, exit the loop
                break

            # Build and send tick notification
            tick_data = self._build_tick_data()

            # Send notification to subscribers
            self.send_notification(tick_data)

            # Send Redis notification
            self.send_redis_notification(self._redis_channel, tick_data)

            self._tick_number += 1

            BBLogger.log(
                f"Tick #{self._tick_number} emitted "
                f"(channel: {self._redis_channel})"
            )

        BBLogger.log(f"Tick loop ended after {self._tick_number} ticks")

    def _stop_monitoring_implementation(self):
        """Stop the tick loop."""
        self._stop_event.set()
        BBLogger.log(f"Tick monitoring stop requested for {self.__class__.__name__}")

    def _build_tick_data(self):
        """
        Build the tick notification data dictionary.

        Returns:
            dict: Tick data including timestamp and metrics
        """
        tick_data = {
            "tick": True,
            "timestamp": time.time(),
            "timestamp_iso": datetime.now().isoformat(),
            "datasource_id": self._datasource_id,
            "datasource_type": self.__class__.__name__,
            "mode": self._mode,
            "tick_number": self._tick_number + 1,
        }

        if self._mode == "cron":
            tick_data["cron"] = self._cron_expression
        else:
            tick_data["time"] = self._time_value
            tick_data["unit"] = self._unit

        if self._start_time is not None:
            tick_data["elapsed_time"] = time.time() - self._start_time

        if self._subscriber_connection:
            tick_data["subscriber"] = self._subscriber_connection

        return tick_data

    def get_tick_count(self):
        """
        Get the current tick count.

        Returns:
            int: Number of ticks emitted since monitoring started
        """
        return self._tick_number

    def get_elapsed_time(self):
        """
        Get elapsed time since monitoring started.

        Returns:
            float: Elapsed time in seconds, or 0 if not started
        """
        if self._start_time is None:
            return 0.0
        return time.time() - self._start_time

    def set_interval(self, time_value=None, unit=None, cron=None):
        """
        Update the tick interval.

        Note: Takes effect on the next tick cycle.
        Provide either (time_value + unit) OR cron, not both.

        Args:
            time_value (float, optional): Numeric interval value.
            unit (str, optional): One of "hour", "min", "sec", "ms".
            cron (str, optional): Cron expression string.
        """
        if cron:
            self._mode = "cron"
            self._cron_expression = cron.strip()
            try:
                from croniter import croniter
                self._cron_iter = croniter(self._cron_expression, datetime.now())
            except Exception as e:
                BBLogger.log(f"Failed to set cron interval: {e}")
                return
            BBLogger.log(f"Tick interval updated to cron: {self._cron_expression}")
        else:
            self._mode = "interval"
            if time_value is not None:
                self._time_value = float(time_value)
            if unit is not None:
                self._unit = unit.lower()
            BBLogger.log(f"Tick interval updated to {self._time_value} {self._unit}")

    def trigger_immediate(self):
        """
        Trigger an immediate tick notification outside the regular interval.

        This is useful for manually triggering dependent data sources.
        """
        tick_data = self._build_tick_data()
        tick_data["immediate"] = True

        BBLogger.log("Triggering immediate tick notification")
        self.send_notification(tick_data)
        self.send_redis_notification(self._redis_channel, tick_data)
        self._tick_number += 1

    def fetch(self):
        """
        Start the tick data source and begin emitting ticks immediately.

        Returns status information about the tick source.
        """
        BBLogger.log(f"Fetch called on tick data source {self.__class__.__name__}")

        self.start_monitoring()

        status = {
            "status": "running" if self._monitoring_active else "ready",
            "datasource": self.get_name(),
            "mode": self._mode,
            "redis_channel": self._redis_channel,
        }

        if self._mode == "cron":
            status["cron"] = self._cron_expression
        else:
            status["time"] = self._time_value
            status["unit"] = self._unit

        if self._subscriber_connection:
            status["subscriber"] = self._subscriber_connection

        return status

    def get_icon(self) -> str:
        """Return SVG icon for the ticker data source."""
        icon_path = os.path.join(os.path.dirname(__file__), "icon.svg")
        try:
            with open(icon_path, "r", encoding="utf-8") as f:
                content = f.read().strip()
                if content:
                    return content
        except Exception as e:
            BBLogger.log(f"Error reading icon file: {e}")

        # Fallback clock icon
        return '''<svg viewBox="0 0 24 24" fill="none" xmlns="http://www.w3.org/2000/svg">
            <circle cx="12" cy="12" r="10" stroke="currentColor" stroke-width="2"/>
            <polyline points="12,6 12,12 16,14" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"/>
        </svg>'''

    def get_connection_data(self) -> dict:
        """Return connection configuration for the ticker data source."""
        return {
            "connection_type": self.connection_type,
            "fields": list(self.connection_fields)
        }
