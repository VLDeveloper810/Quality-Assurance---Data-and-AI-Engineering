import json
import logging
import time
from datetime import datetime

# Create a dedicated logger instance
logger = logging.getLogger("LakehouseMonitor")
logger.setLevel(logging.INFO)
logger.propagate = False  # Prevents duplicate log printing to Spark console

# Clear out any default handlers if they exist
if logger.hasHandlers():
    logger.handlers.clear()

# Build explicit, isolated layout formatting
log_formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s")

# Stream Handler (To print visibly to your active Terminal Screen)
screen_handler = logging.StreamHandler()
screen_handler.setFormatter(log_formatter)
logger.addHandler(screen_handler)

# File Handler (To save logs to your persistent pipeline_metrics.log file)
file_handler = logging.FileHandler("pipeline_metrics.log", mode="a", encoding="utf-8")
file_handler.setFormatter(log_formatter)
logger.addHandler(file_handler)


class PipelineMonitor:
    def __init__(self):
        self.metrics = {}
        self.start_times = {}

    def start_timer(self, stage_name: str):
        """Tracks the start time for SLA and Latency evaluation."""
        self.start_times[stage_name] = time.time()

    def stop_timer(self, stage_name: str):
        """Calculates duration and logs it against your operational SLA."""
        if stage_name in self.start_times:
            duration = time.time() - self.start_times[stage_name]
            self.metrics[f"{stage_name}_duration_seconds"] = round(duration, 3)
            logger.info(
                f"[SLA TRACKING] Stage '{stage_name}' completed in {duration:.3f} seconds."
            )
            return duration
        return 0

    def detect_entity_drift(
        self,
        match_rate: float,
        historical_average: float = 65.0,
        threshold_deviation: float = 15.0,
    ):
        """
        Anomaly Detection: Evaluates if the current fuzzy-match rate deviates
        unusually far from historical baseline trends (Entity Drift).
        """
        self.metrics["fuzzy_match_rate_pct"] = round(match_rate, 2)

        lower_bound = historical_average - threshold_deviation
        upper_bound = historical_average + threshold_deviation

        logger.info(
            f"[MONITOR] Current Match Rate: {match_rate:.2f}% (Historical Baseline: {historical_average}%)"
        )

        if match_rate < lower_bound or match_rate > upper_bound:
            logger.warning(
                f"[ANOMALY DETECTED] Entity Drift Alert! Match rate of {match_rate:.2f}% "
                f"deviates sharply from historical expected limits ({lower_bound}% - {upper_bound}%)."
            )
            self.metrics["entity_drift_triggered"] = True
        else:
            logger.info(
                "[HEALTH] Entity match rates fall within safe statistical boundaries."
            )
            self.metrics["entity_drift_triggered"] = False

    def export_structured_health_dashboard(self, ml_metrics: dict = None):
        """Exports pipeline operational telemetry in an immutable JSON block to log and screen."""
        if ml_metrics:
            # Bind versioned model data into our core telemetry block
            self.metrics["model_version"] = ml_metrics.get("version", "v1.0.0")
            self.metrics["model_accuracy_auc"] = round(
                ml_metrics.get("accuracy", 0.0), 4
            )
            self.metrics["historical_baseline_accuracy"] = round(
                ml_metrics.get("baseline", 0.85), 4
            )

        dashboard = {
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "pipeline_health_status": (
                "CRITICAL" if self.metrics.get("entity_drift_triggered") else "HEALTHY"
            ),
            "telemetry": self.metrics,
        }

        border = "=" * 69
        title = "STRUCTURED HEALTH DASHBOARD (MOCK PROMETHEUS METRIC EXPORT)"
        json_body = json.dumps(dashboard, indent=4)

        logger.info(f"\n{border}\n{title}\n{border}\n{json_body}\n{border}\n")
        """Exports pipeline operational telemetry in an immutable JSON block to log and screen."""
        dashboard = {
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "pipeline_health_status": (
                "CRITICAL" if self.metrics.get("entity_drift_triggered") else "HEALTHY"
            ),
            "telemetry": self.metrics,
        }

        # Format beautiful string lines
        border = "=" * 69
        title = "STRUCTURED HEALTH DASHBOARD (MOCK PROMETHEUS METRIC EXPORT)"
        json_body = json.dumps(dashboard, indent=4)

        # Log via our dedicated channels so it hits BOTH screen and file seamlessly
        logger.info(f"\n{border}\n{title}\n{border}\n{json_body}\n{border}\n")
