# Part 3: Tasks

We implement four tasks, building in complexity. Each demonstrates a
different task pattern.

## Custom lock type

First, register a custom locked-object type. Create
`extensions/gubbins/gubbins-tasks/src/gubbins/tasks/my_pilot_lock_types.py`:

<!-- blacken-docs:off -->

```python
--8<-- "extensions/gubbins/gubbins-tasks/src/gubbins/tasks/my_pilot_lock_types.py"
```

<!-- blacken-docs:on -->

## Task implementations

Create `extensions/gubbins/gubbins-tasks/src/gubbins/tasks/my_pilots.py`:

<!-- blacken-docs:off -->

```python
--8<-- "extensions/gubbins/gubbins-tasks/src/gubbins/tasks/my_pilots.py"
```

<!-- blacken-docs:on -->

### MyPilotTask (one-shot)

The simplest task. It receives a `ce_name`, reads the CE's
`success_rate` from the DB, rolls `random.random()`, and either
submits a pilot or raises an exception.

- **Locks**: `MutexLock(MY_PILOT, ce_name)` — serialises submissions
    to the same CE. Different CEs can submit in parallel.
- **Retry**: `NoRetry()` — the periodic parent will naturally
    resubmit on the next cycle
- **DLQ**: `dlq_eligible = True` — failures can be inspected

### MyPilotReportTask (periodic, non-VO-aware)

A periodic task that runs on a `CronSchedule("0 * * * *")` (hourly).
It calls `get_pilot_summary()` and logs the result.

Because it's not VO-aware, it reports aggregate statistics across all
VOs. Only one instance runs at a time (default mutex lock).

### MyCheckPilotsTask (VO-aware periodic)

Runs every 30 seconds per VO. Transitions:

- `SUBMITTED → RUNNING` (always, immediately)
- `RUNNING → DONE` (if `random.random() < success_rate`)
- `RUNNING → FAILED` (otherwise)

The VO-scoped lock ensures each VO's pilots are checked independently.

### MySubmitPilotsTask (VO-aware periodic, spawns children)

The most complex task. Runs every 60 seconds per VO:

1. Queries `get_available_ces()` for CEs with remaining capacity
2. Spawns a `MyPilotTask` per available slot via `await task.schedule()`

This demonstrates the parent-child task pattern — a periodic task
that creates one-shot tasks dynamically.

## Register entry points

Add the following to `extensions/gubbins/gubbins-tasks/pyproject.toml`.

Task entry points:

```toml
--8<-- "extensions/gubbins/gubbins-tasks/pyproject.toml:my_pilots_task_entry_points"
```

Lock type entry point (add under `[project.entry-points."diracx.lock_object_types"]`):

```toml
--8<-- "extensions/gubbins/gubbins-tasks/pyproject.toml:my_pilots_lock_entry_point"
```
