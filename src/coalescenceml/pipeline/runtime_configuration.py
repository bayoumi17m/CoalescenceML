from __future__ import annotations

from typing import TYPE_CHECKING, Any, Dict, Optional, cast

from coalescenceml.constants import RUN_NAME_OPTION_KEY
from coalescenceml.logger import get_logger


if TYPE_CHECKING:
    from coalescenceml.pipeline import Schedule

logger = get_logger(__name__)
SCHEDULE_OPTION_KEY = "schedule"


class RuntimeConfiguration(Dict[str, Any]):
    """RuntimeConfiguration store dynamic options for a pipeline run.
    Use `stack.runtime_options()` to get all available runtime options for the
    components of a specific CoalescenceML stack.
    This class is a `dict` subclass, so getting/setting runtime options is done
    using `some_value = runtime_configuration["some_key"]` and
    `runtime_configuration["some_key"] = 1`.
    """

    def __init__(
        self,
        *,
        run_name: Optional[str] = None,
        schedule: Optional["Schedule"] = None,
        **runtime_options: Any,
    ):
        """Initializes a RuntimeConfiguration object.
        Args:
            run_name: Optional name of the pipeline run.
            schedule: Optional schedule of the pipeline run.
            **runtime_options: Additional runtime options.
        """
        runtime_options[RUN_NAME_OPTION_KEY] = run_name
        runtime_options[SCHEDULE_OPTION_KEY] = schedule
        super().__init__(runtime_options)

    @property
    def run_name(self) -> Optional[str]:
        """Name of the pipeline run."""
        return cast(Optional[str], self[RUN_NAME_OPTION_KEY])

    @property
    def schedule(self) -> Optional[Schedule]:
        """Schedule of the pipeline run."""
        from coalescenceml.pipeline import Schedule

        return cast(Optional[Schedule], self[SCHEDULE_OPTION_KEY])
