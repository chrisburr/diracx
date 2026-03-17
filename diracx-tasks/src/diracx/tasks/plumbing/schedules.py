from __future__ import annotations

__all__ = [
    "TaskScheduleBase",
    "IntervalSeconds",
    "CronSchedule",
    "RRuleSchedule",
]

from abc import ABC, abstractmethod
from datetime import UTC, datetime, timedelta


class TaskScheduleBase(ABC):
    """Abstract base class for task scheduling."""

    @abstractmethod
    def next_occurrence(self) -> datetime:
        """Return the datetime for the next scheduled occurrence."""
        ...


class IntervalSeconds(TaskScheduleBase):
    """Schedule that repeats at a fixed interval."""

    def __init__(self, seconds: int):
        self.seconds = seconds

    def next_occurrence(self) -> datetime:
        return datetime.now(tz=UTC) + timedelta(seconds=self.seconds)


class CronSchedule(TaskScheduleBase):
    """Schedule based on a cron expression."""

    def __init__(self, expression: str):
        self.expression = expression

    def next_occurrence(self) -> datetime:
        from croniter import croniter

        cron = croniter(self.expression, datetime.now(tz=UTC))
        return cron.get_next(datetime)


class RRuleSchedule(TaskScheduleBase):
    """Schedule based on an RFC 2445 recurrence rule."""

    def __init__(self, rule: str):
        self.rule = rule

    def next_occurrence(self) -> datetime:
        from dateutil.rrule import rrulestr

        rrule = rrulestr(self.rule, dtstart=datetime.now(tz=UTC))
        result = rrule.after(datetime.now(tz=UTC))
        if result is None:
            raise ValueError(f"RRule {self.rule!r} has no future occurrences")
        return result
