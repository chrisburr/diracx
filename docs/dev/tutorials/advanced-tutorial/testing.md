# Part 5: Testing

DiracX follows a layered testing approach: each component (database,
tasks, router) is tested independently. This catches issues at the
narrowest possible scope and keeps tests fast.

We use a two-CE pattern throughout for deterministic testing:
`success_rate=1.0` always succeeds, `success_rate=0.0` always fails.
This avoids flaky tests from random outcomes.

## Database tests

<!-- blacken-docs:off -->

```python title="gubbins-db/tests/test_my_pilot_db.py"
--8<-- "extensions/gubbins/gubbins-db/tests/test_my_pilot_db.py"
```

<!-- blacken-docs:on -->

The fixture pattern here is common across DiracX database tests:

- **In-memory SQLite** —
    `MyPilotDB("sqlite+aiosqlite:///:memory:")` creates a throwaway
    database for each test. No cleanup needed.
- **Engine lifecycle** — `engine_context()` manages the async engine;
    `engine.begin()` creates the tables from your schema.
- **Transactions** — Each `async with db:` block opens and commits a
    separate transaction.

!!! tip "Each `async with db:` is a separate transaction"

    This is important for testing: if you insert data in one
    `async with db:` block and query it in another, you're testing that the
    data was actually committed — not just visible within the same
    transaction. This mirrors how the production code works (each HTTP
    request or task execution gets its own transaction).

Run:

```bash
pixi run pytest-gubbins-db -- -k my_pilot
```

## Task tests

<!-- blacken-docs:off -->

```python title="gubbins-tasks/tests/test_my_pilot_tasks.py"
--8<-- "extensions/gubbins/gubbins-tasks/tests/test_my_pilot_tasks.py"
```

<!-- blacken-docs:on -->

Task tests verify four things:

1. **Properties** — Priority, size, retry policy, DLQ eligibility
2. **Locks** — That `redis_key` contains expected components and
    different args produce different lock keys
3. **Serialization** — That the dataclass round-trips correctly
    (implicit in the constructor tests)
4. **Execution** — That `execute()` calls the right DB methods with the
    right arguments

!!! note "Why mock the database?"

    Task tests use `AsyncMock()` for the database instead of a real
    connection. This is a deliberate choice: the database layer has its own
    tests (above), so task tests focus purely on the task's logic —
    branching, error handling, and which DB methods get called. This keeps
    the test fast and the failure messages precise.

!!! tip "Deterministic randomness"

    The tests use `success_rate=1.0` and `success_rate=0.0` to make
    `random.random()` comparisons deterministic. With `success_rate=1.0`,
    `random.random() >= 1.0` is always `False` (success). With
    `success_rate=0.0`, it's always `True` (failure). No need to mock
    `random` — the math does the work.

Run:

```bash
pixi run pytest-gubbins-tasks -- -k my_pilot
```

## Router tests

<!-- blacken-docs:off -->

```python title="gubbins-routers/tests/test_my_pilots.py"
--8<-- "extensions/gubbins/gubbins-routers/tests/test_my_pilots.py"
```

<!-- blacken-docs:on -->

Router tests use DiracX's test client infrastructure:

- **`enabled_dependencies`** — Lists the dependency classes the test
    client should wire up. This spins up an in-memory database and
    injects it into the router endpoints.
- **`client_factory`** — Provides authenticated HTTP clients that
    already have valid tokens, so you can focus on testing the endpoint
    logic.
- **URL prefix** — The entry point name `my_pilots` maps to
    `/api/my_pilots/`, so all requests go through that prefix.

For more testing patterns, see
[Writing tests](../../reference/writing-tests.md) and
[Test recipes](../../reference/test-recipes.md).

Run:

```bash
pixi run pytest-gubbins-routers -- -k my_pilots
```

## Final checkpoint

Run all tutorial tests to verify everything works together:

```bash
pixi run test-tutorial
```
