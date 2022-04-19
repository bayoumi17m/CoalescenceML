import datetime
from typing import Optional

from pydantic import BaseModel


class Schedule(BaseModel):
    """Class for defining a pipeline schedule.
    Attributes:
        start_time: Datetime object to indicate when to start the schedule.
        end_time: Datetime object to indicate when to end the schedule.
        interval_second: Integer indicating the seconds between two recurring
            runs in for a periodic schedule.
        catchup: Whether the recurring run should catch up if behind schedule.
            For example, if the recurring run is paused for a while and
            re-enabled afterwards. If catchup=True, the scheduler will catch
            up on (backfill) each missed interval. Otherwise, it only
            schedules the latest interval if more than one interval is ready to
            be scheduled. Usually, if your pipeline handles backfill
            internally, you should turn catchup off to avoid duplicate backfill.
    """

    start_time: datetime.datetime
    end_time: Optional[datetime.datetime] = None
    interval_second: int
    catchup: bool = False

    @property
    def utc_start_time(self) -> str:
        """ISO-formatted string of the UTC start time."""
        return self.start_time.astimezone(datetime.timezone.utc).isoformat()

    @property
    def utc_end_time(self) -> Optional[str]:
        """Optional ISO-formatted string of the UTC end time."""
        if not self.end_time:
            return None

        return self.end_time.astimezone(datetime.timezone.utc).isoformat()
