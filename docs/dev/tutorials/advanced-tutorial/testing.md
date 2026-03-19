# Part 5: Testing

Each layer has its own test patterns. We use the two-CE pattern for
deterministic testing: `success_rate=1.0` always succeeds,
`success_rate=0.0` always fails.

## Database tests

Create `extensions/gubbins/gubbins-db/tests/test_my_pilot_db.py`:

<!-- blacken-docs:off -->

```python
--8<-- "extensions/gubbins/gubbins-db/tests/test_my_pilot_db.py"
```

<!-- blacken-docs:on -->

Key patterns:

- **In-memory SQLite**: `MyPilotDB("sqlite+aiosqlite:///:memory:")`
- **Engine lifecycle**: `engine_context()` manages the engine; `engine.begin()` creates tables
- **Transactions**: each `async with db:` block is a separate transaction

Run:

```bash
pixi run pytest-gubbins-db -- -k my_pilot
```

## Task tests

Create `extensions/gubbins/gubbins-tasks/tests/test_my_pilot_tasks.py`:

<!-- blacken-docs:off -->

```python
--8<-- "extensions/gubbins/gubbins-tasks/tests/test_my_pilot_tasks.py"
```

<!-- blacken-docs:on -->

Key patterns:

- **Lock verification**: check `redis_key` contains expected components
- **Lock isolation**: different args produce different lock keys
- **Mocked DB**: `AsyncMock()` for unit-testing `execute()` without a real database
- **Deterministic randomness**: `success_rate=1.0` means `random.random() >= 1.0` is always `False` (success); `success_rate=0.0` means always `True` (failure)

Run:

```bash
pixi run pytest-gubbins-tasks -- -k my_pilot
```

## Router tests

Create `extensions/gubbins/gubbins-routers/tests/test_my_pilots.py`:

<!-- blacken-docs:off -->

```python
--8<-- "extensions/gubbins/gubbins-routers/tests/test_my_pilots.py"
```

<!-- blacken-docs:on -->

Key patterns:

- **`enabled_dependencies`**: lists the dependency classes the test client needs
- **`client_factory`**: provides authenticated HTTP clients
- **URL prefix**: entry point name `my_pilots` maps to `/api/my_pilots/`

Run:

```bash
pixi run pytest-gubbins-routers -- -k my_pilots
```
