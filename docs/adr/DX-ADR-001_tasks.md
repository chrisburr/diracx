# DX-ADR-001: Tasks

## Metadata

- **Created By:** Chris Burr, Christophe Haen
- **Date:** 2026-03-11
- **Status:** Draft
- **Decision Maker(s):** Alexandre Boyer

## Abstract

TODO

## Motivation

[Why is this decision needed now? What problem or limitation in the current system does it address? What are the functional and non-functional drivers?]

## Specification

### Tasks definition

```python
class BaseTask:
    priority  # BACKGROUND/NORMAL/REALTIME
    size  # SMALL/MEDIUM/LARGE
    retry_policy: RetryPolicyBase

    @property
    def execution_locks(self) -> list[BaseLock]:
        """Return a list of lock keys required by this task."""
        # Limiters are not applied by default however their presence allows them
        # to be configured via the central configuration repository.
        return [
            RateLimiter(LockedObjectType.TASK, self.__class__.__name__),
            ConcurrencyLimiter(LockedObjectType.TASK, self.__class__.__name__),
        ]

    async def execute(self):
        """Main execution logic of the task."""
```

```python
class RetryPolicyBase:
    def schedule_retry(self, attempt: int, exception: Exception) -> datetime | None:
        """Return the datetime for the next retry, or None to not retry."""
```

#### Arguments

Tasks can define additional arguments which serialized and stored in the task system.
These are passed as arguments to `BaseTask.__init__` and the subclass of `BaseTask` is expected to be serializable (e.g. by using `@dataclass` or `pydantic.BaseModel`).

```python
from dataclasses import dataclass


@dataclass
class ExampleTask(BaseTask):
    arg1: str
    arg2: int
```

```python
from pydantic import BaseModel


class AnotherExampleTask(BaseTask, BaseModel):
    arg1: str
    arg2: int
```

#### Dependency Injection

Subclasses of `BaseTask` can define additional arguments which are passed at task-execution time in a similar way to FastAPI's dependency injection system used in `diracx-routers`.
These are defined as arguments to `BaseTask.execute` and are expected to be type annotated with a class which is registered in the dependency injection system.

```python
from diracx.tasks.depends import AuthSettings, JobDB


class ExampleTask(BaseTask):
    async def execute(self, job_db: JobDB, auth_settings: AuthSettings):
        # Use job_db to perform database operations related to this task
        pass
```

The classes which have been annotated for dependency injection are re-exported in `diracx.routers.depends`.

#### Locking

```python
class LockedObjectType(str):
    """String representing the type of object being locked.

    e.g. "job", "transformation", "task", "lfn", etc.
    """
```

```python
class BaseLock:
    def __init__(self, obj: LockedObjectType, key: int | str):
        """Base class for locks.

        Args:
            obj: The type of object being locked.
            key: The key identifying the locked object.
        """
        self.obj = obj
        self.key = key

    @abstractmethod
    async def acquire(self, conn):
        ...

    @abstractmethod
    async def release(self, conn):
        ...
```

#### Limiters

A subset of locks are "limiters", which are only applied for non-interactive task execution.
At the time of writing two such limiters are foreseen:

```python
class RateLimiter:
    """Enforces a maximum number of operations within a given time window."""

    limit: int | None  # Number of allowed operations within the time window
    window_seconds: int | None
```

```python
class ConcurrencyLimiter:
    """Enforces a maximum number of concurrent operations."""

    limit: int | None  # Maximum number of concurrent operations
```

#### Periodic Tasks

Period tasks can be split into two categories:

- Tasks which run once per installation (e.g. cleaning the sandbox store)
- Tasks which run once per VO (e.g. pilot submission)

Periodic tasks are scheduled to run at specific intervals or times depending on a schedule property which is defined with this base class:

```python
class TaskScheduleBase:
    """Abstract base class for task scheduling."""

    def next_occurrence(self) -> datetime:
        """Return the datetime for the next scheduled occurrence."""
```

Initially, we foresee three implementations of this base class:

- `IntervalSecondsSchedule`: runs at fixed intervals defined in seconds (e.g. every 3600 seconds)
- `CronSchedule`: runs at specific times defined by a cron expression (e.g. every day at midnight)
- `RRuleSchedule`: runs at specific times defined by an iCalendar RFC5545 RRULE expression (e.g. every last Friday of the month at 5pm)

The `PeriodicBaseTask` extends a pydantic model such that it's arguments can be serialized with two private attributes which can be overridden by the configuration.

```python
class PeriodicBaseTask(BaseModel):
    """Base class for periodic tasks."""

    _schedule: TaskScheduleBase  # The default schedule for this task
    _enabled: bool = True  # Whether this periodic task is enabled by default

    @staticmethod
    def validate_config(config: list[TaskConfig]) -> None:
        """Validate the configuration for this periodic task.

        Raises a ValueError if the configuration is found to be invalid.
        """
        return

    @property
    def execution_locks(self) -> list[BaseLock]:
        """Periodic tasks cannot be executed concurrently unless subclasses opt-out."""
        # This intentionally does not call super() as the default limiters are
        # strictly more permissive than the schedule + mutex combination.
        return [
            MutexLock(LockedObjectType.TASK, self.__class__.__name__),
        ]
```

```python
class PeriodicVoAwareBaseTask(PeriodicBaseTask):
    """Base class for periodic tasks which are VO-aware."""

    vo: str

    @property
    def execution_locks(self) -> list[BaseLock]:
        """Periodic tasks cannot be executed concurrently unless subclasses opt-out."""
        # This intentionally does not call super() as the default limiters are
        # strictly more permissive than the schedule + mutex combination.
        return [
            MutexLock(LockedObjectType.TASK, self.__class__.__name__, self.vo),
        ]
```

```python
class SubmitPilots(PeriodicVoAwareBaseTask):
    def __init__(self, *, vo: str, ce_regex: str | None = None):
        super().__init__(vo=vo, ce_regex=ce_regex)


class SubmitPilots(PeriodicVoAwareBaseTask):
    def __init__(self, *, vo: str, ce_regex: str | None = None):
        super().__init__(vo=vo)
        self.ce_regex = re.compile(ce_regex)


class SubmitPilots(PeriodicVoAwareBaseTask):
    ce_regex: re.Pattern = re.compile(".*")
```

### Retries

Retries can originate from several sources:

- Retry policy: if a task raises an exception during execution, the retry policy is consulted to determine if and when to retry the task. This results in the tasks's execution count being incremented and an error being logged.
- Lock acquisition failure: if a task fails to acquire one of its locks, it will be retried after a delay which increases with the number of consecutive lock acquisition failures.

### Configuration

Configuration for tasks has two sources of truth:

- The properties on a task (and the recursive properties of non-primitive types such as locks and retry policies) define the default configuration for that task.
- Defaults can be overridden by a file which is stored in the central configuration repository. This file is expected to be in YAML format and follow a structure which mirrors the properties of the task classes.

Limiter defaults should provide a sensible default behaviour for installations, under the assumption that most admins might not be familiar with the task system internals.
These defaults have two contrasting goals:

- Avoid overloading systems due to too much concurrency hitting external systems (e.g. storage elements)
- Avoid tasks not being scheduled due to limiters being overly restrictive.

The configuration for periodic tasks should allow for the schedule to be overridden either for all occurrences of the task or on a per-VO basis.
If a task class is defined in both `common` and `vo-overrides.EXAMPLE_VO` then the `common` configuration is ignored.

```yaml
common:
  periodic-tasks:
    SubmitPilots:
      - args:
        ce_regex: .*
        enabled: True

  limits:
    Task:
      SubmitPilots:
        ConcurrencyLimiter:
          limit: 2
        RateLimiter:
          limit: 10
          window_seconds: 3600

    StorageElement:
       default:
          CreateFile:
            ConcurrencyLimiter:
              limit: 5
          RemoveFile:
            ConcurrencyLimiter:
              limit: 5
       "CERN-DST":
          RemoveFile:
            ConcurrencyLimiter:
              limit: 5
       "CERN-MC_DST":
          RemoveFile:
            ConcurrencyLimiter:
              limit: 5

    Transformation:
      default:
        ConcurrencyLimiter:
          limit: 10
      "12345":  # This transformation is only allowed to consume 5 slots
        ConcurrencyLimiter:
          limit: 5

vo-overrides:
  lhcb:
    periodic-tasks:
      SubmitPilots:
        - name: SubmitPilotsCERN
          schedule:
            class: RRuleSchedule
            arg: "FREQ=HOURLY;INTERVAL=2"
          args:
            ce_regex: .*.cern.ch
        - name: SubmitPilotsRAL
          schedule:
            class: CronSchedule
            arg: "0 */6 * * *"
          args:
            ce_regex: .*.ral.ac.uk
        - args:
            ce_regex: ~(*.cern.ch|*.ral.ac.uk)

  limits:
    ConcurrencyLimiter:
      Task:
        CheckPilotStatus:
          limit: 10_000
          window_seconds: 3600
```

```python
class RemoveFile(BaseTask):
    storage_element: str
    lfn: str

    def execution_locks(self) -> list[BaseLock]:
        return super().execution_locks + [
            MutexLock(LockedObjectType.LFN, self.lfn),
            ConcurrencyLimiter(
                LockedObjectType.STORAGE_ELEMENT,
                self.storage_element,
                StorageElementAction.RemoveFile,
            ),
        ]
```

### Scheduling

Only a single instance of the scheduler can be running at any given time, this is enforced both as a `StatefulSet` in kubernetes and with a mutex lock in the redis instance itself.
The scheduler serves two purposes:

- Scheduling tasks for execution by adding them to the pending tasks stream at the appropriate time.
- Adding periodic tests to the LIST of scheduled tasks at startup and when their schedule changes. Each time a periodic task is moved to the pending tasks stream, the next occurrence is immediately scheduled.

When the periodic task configuration is updated, the scheduler should remove the task from the pending tasks LIST if it's already there and add it again in the appropriate key.
Some additional metadata should be stored in the broker to allow for the scheduler to determine if a task needs to be rescheduled when the configuration changes (e.g. schedule, next occurrence, etc).

### Broker

The pending tasks is implemented as nine Redis streams, one per priority (BACKGROUND, NORMAL, REALTIME) and size (SMALL, MEDIUM, LARGE) combination.
This allows us to have three classes of workers which can be scaled independently and have different resource requirements, each consuming from the appropriate three priority streams.
The task scheduler is responsible for adding tasks to the appropriate stream based on their priority and size.

The state of the broker should be ephemeral and recreated with each update. Any persistent state should be stored in the standard MySQL database's used by DiracX. This requirement is imposed to:

- Simplify recovery from unexpected outages.
- Reduce the complexity of reasoning about updates which may change details of the broker's internal state.
- Improve performance by removing the need to ensure every action is flushed to persistent storage.

Upon first start, the broker is populated with the cron-style tasks as well as any pending reactive tasks that have been persisted in MySQL. The tasks which were persisted are those eligible for the dead letter queue.

## Rationale

### `MutexLock` vs `ConcurrencyLimiter`

Explain *why* the chosen design looks the way it does. Why these trade-offs? Why this level of abstraction? Connect specific design choices back to the drivers in Motivation.

### `MutexLock` for Periodic Tasks

### Dependency Injection

`diracx.routers.depends` re-exports `diracx.tasks.depends` due to the following reasoning:

- We don't want `diracx-logic`/`diracx-db` to depend on `fastapi`
- `diracx-tasks` shouldn't depend on `diracx-routers`
- Pragmatically, `diracx-tasks` will always be installed alongside `diracx-routers` so we can reuse the same dependency injection system without introducing a new one just for tasks.
- Importing from within the same subpackage hides this implementation detail and allows us to change the implementation in the future without breaking compatibility.

### Resource Status Restrictions

## Rejected Ideas

### Why not a third party library (e.g. Celery, taskiq, dramatiq, ...)?

## Open Issues

[Any points still being decided or discussed. Remove this section once the status moves to Accepted.]
