# File: SubjectiveTickerDataSource.py

import os
import time
import threading
from datetime import datetime
from subjective_abstract_data_source_package import SubjectiveRealTimeDataSource
from brainboost_data_source_logger_package.BBLogger import BBLogger


class SubjectiveTickerDataSource(SubjectiveRealTimeDataSource):
    """
    A real-time data source that triggers ticks at configurable intervals.

    This data source emits "tick" events that can be used to trigger downstream
    data sources in a pipeline. It supports three timing modes:

    1. Seconds-based: tick every N seconds
    2. Minutes-based: tick every N minutes
    3. Cron expression: tick according to a cron schedule

    Each tick emits:
    - A notification to all subscriber data sources
    - A Redis signal on the configured channel

    Parameters (via params dict):
        interval_type (str): "seconds", "minutes", or "cron". Default: "seconds"
        interval_value (float/str):
            - For seconds/minutes: numeric value (e.g., 30 for 30 seconds)
            - For cron: cron expression string (e.g., "*/5 * * * *" for every 5 minutes)
        tick_params (dict): Additional parameters to include in each tick notification
        tick_count (int): Maximum number of ticks to emit. -1 for unlimited. Default: -1
        include_tick_number (bool): Include tick count in notifications. Default: True
        include_elapsed_time (bool): Include elapsed time in notifications. Default: True
        auto_start (bool): Start ticking on fetch(). Default: True
        redis_channel (str): Redis channel for tick notifications. Default: "ticker_events"
        datasource_id (str): Identifier for this ticker instance

    Example usage:
        ```python
        # Tick every 30 seconds
        ticker = SubjectiveTickerDataSource(
            name="my_ticker",
            params={
                "interval_type": "seconds",
                "interval_value": 30,
                "redis_channel": "pipeline_triggers"
            }
        )

        # Tick every 5 minutes using cron
        ticker = SubjectiveTickerDataSource(
            name="cron_ticker",
            params={
                "interval_type": "cron",
                "interval_value": "*/5 * * * *",
                "tick_params": {"source": "scheduler"}
            }
        )
        ```
    """

    connection_type = "Ticker"
    connection_fields = [
        "interval_type",
        "interval_value",
        "tick_count",
        "tick_params",
        "auto_start"
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

        # Timing configuration
        self._interval_type = self.params.get("interval_type", "seconds").lower()
        self._interval_value = self.params.get("interval_value", 60)

        # Tick configuration
        self._tick_params = self.params.get("tick_params", {}) or {}
        self._tick_count_limit = self._coerce_int_param("tick_count", -1)
        self._include_tick_number = self._coerce_bool_param("include_tick_number", True)
        self._include_elapsed_time = self._coerce_bool_param("include_elapsed_time", True)
        self._auto_start = self._coerce_bool_param("auto_start", True)

        # Redis configuration
        self._redis_channel = self.params.get("redis_channel", "ticker_events")
        self._datasource_id = self.params.get("datasource_id", name or "ticker")

        # Runtime state
        self._tick_number = 0
        self._start_time = None
        self._stop_event = threading.Event()
        self._cron_iter = None

        BBLogger.log(
            f"SubjectiveTickerDataSource initialized: "
            f"type={self._interval_type}, value={self._interval_value}"
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
        Calculate the interval in seconds based on interval_type.

        Returns:
            float: Interval in seconds
        """
        if self._interval_type == "seconds":
            return float(self._interval_value)
        elif self._interval_type == "minutes":
            return float(self._interval_value) * 60
        elif self._interval_type == "cron":
            # For cron, we calculate dynamically in the tick loop
            return None
        else:
            BBLogger.log(f"Unknown interval_type '{self._interval_type}', defaulting to 60 seconds")
            return 60.0

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
        if self._interval_type == "cron":
            try:
                from croniter import croniter
                self._cron_iter = croniter(str(self._interval_value), datetime.now())
                BBLogger.log(f"Cron expression initialized: {self._interval_value}")
            except ImportError:
                BBLogger.log("croniter not installed, falling back to 60 second interval")
                self._interval_type = "seconds"
                self._interval_value = 60
            except Exception as e:
                BBLogger.log(f"Invalid cron expression '{self._interval_value}': {e}")
                self._interval_type = "seconds"
                self._interval_value = 60

        BBLogger.log(f"Tick monitoring initialized for {self.__class__.__name__}")

    def _start_monitoring_implementation(self):
        """
        Main tick loop that emits notifications at configured intervals.

        This runs in a background thread and sends tick notifications to all
        subscribers and via Redis at each tick.
        """
        interval_desc = (
            f"cron({self._interval_value})"
            if self._interval_type == "cron"
            else f"{self._interval_value} {self._interval_type}"
        )
        BBLogger.log(f"Starting tick loop: {interval_desc}")

        while self._monitoring_active and not self._stop_event.is_set():
            # Check tick count limit
            if self._tick_count_limit > 0 and self._tick_number >= self._tick_count_limit:
                BBLogger.log(f"Tick count limit reached ({self._tick_count_limit}), stopping")
                break

            # Calculate wait time based on interval type
            if self._interval_type == "cron":
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
            dict: Tick data including timestamp, configured params, and metrics
        """
        tick_data = {
            "tick": True,
            "timestamp": time.time(),
            "timestamp_iso": datetime.now().isoformat(),
            "datasource_id": self._datasource_id,
            "datasource_type": self.__class__.__name__,
            "interval_type": self._interval_type,
            "interval_value": self._interval_value,
        }

        # Add tick number if configured
        if self._include_tick_number:
            tick_data["tick_number"] = self._tick_number + 1  # 1-indexed for display

        # Add elapsed time if configured
        if self._include_elapsed_time and self._start_time is not None:
            tick_data["elapsed_time"] = time.time() - self._start_time

        # Add tick count limit info if set
        if self._tick_count_limit > 0:
            tick_data["remaining_ticks"] = self._tick_count_limit - self._tick_number - 1

        # Merge in user-provided tick parameters
        if self._tick_params:
            tick_data.update(self._tick_params)

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

    def set_interval(self, interval_type, interval_value):
        """
        Update the tick interval.

        Note: Takes effect on the next tick cycle.

        Args:
            interval_type (str): "seconds", "minutes", or "cron"
            interval_value: Numeric value or cron expression
        """
        self._interval_type = interval_type.lower()
        self._interval_value = interval_value

        # Reinitialize cron iterator if switching to cron mode
        if self._interval_type == "cron":
            try:
                from croniter import croniter
                self._cron_iter = croniter(str(interval_value), datetime.now())
            except Exception as e:
                BBLogger.log(f"Failed to set cron interval: {e}")
                return

        BBLogger.log(f"Tick interval updated to {interval_value} {interval_type}")

    def set_tick_params(self, tick_params):
        """
        Update the parameters included in tick notifications.

        Args:
            tick_params (dict): Parameters to merge into tick notifications
        """
        self._tick_params = tick_params or {}
        BBLogger.log(f"Tick params updated: {self._tick_params}")

    def trigger_immediate(self, extra_params=None):
        """
        Trigger an immediate tick notification outside the regular interval.

        This is useful for manually triggering dependent data sources.

        Args:
            extra_params (dict, optional): Additional parameters for this tick only
        """
        tick_data = self._build_tick_data()
        tick_data["immediate"] = True

        if extra_params:
            tick_data.update(extra_params)

        BBLogger.log("Triggering immediate tick notification")
        self.send_notification(tick_data)
        self.send_redis_notification(self._redis_channel, tick_data)
        self._tick_number += 1

    def fetch(self):
        """
        Start the tick data source.

        If auto_start is True (default), begins emitting ticks immediately.
        Returns status information about the tick source.
        """
        BBLogger.log(f"Fetch called on tick data source {self.__class__.__name__}")

        if self._auto_start:
            self.start_monitoring()

        return {
            "status": "running" if self._monitoring_active else "ready",
            "datasource": self.get_name(),
            "interval_type": self._interval_type,
            "interval_value": self._interval_value,
            "tick_count_limit": self._tick_count_limit,
            "redis_channel": self._redis_channel
        }

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
