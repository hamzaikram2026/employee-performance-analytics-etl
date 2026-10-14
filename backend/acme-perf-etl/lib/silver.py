"""Silver layer: implemented in the next step."""
from .logging_utils import get_logger, log_event

logger = get_logger(__name__)


def run(cfg, lake, audit) -> None:
    log_event(logger, "stage_not_implemented", stage="silver")