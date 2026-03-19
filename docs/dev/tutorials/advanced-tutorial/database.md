# Part 2: Database

We create a new `MyPilotDB` class from scratch (not extending an
existing DB). This shows the full lifecycle of adding a database to
DiracX.

## Schema

Create `extensions/gubbins/gubbins-db/src/gubbins/db/sql/my_pilot_db/schema.py`:

<!-- blacken-docs:off -->

```python
--8<-- "extensions/gubbins/gubbins-db/src/gubbins/db/sql/my_pilot_db/schema.py"
```

<!-- blacken-docs:on -->

Key points:

- `MyPilotStatus` is a `StrEnum` — stored as a string in the DB
- Each table has its own `DeclarativeBase` subclass (`Base`)
- `datetime_now` provides a server-default UTC timestamp
- `str255` maps to `String(255)` via the `type_annotation_map`
- Foreign key links `MyPilotSubmissions.ce_name` to `MyComputeElements.name`

## DB class

Create `extensions/gubbins/gubbins-db/src/gubbins/db/sql/my_pilot_db/db.py`:

<!-- blacken-docs:off -->

```python
--8<-- "extensions/gubbins/gubbins-db/src/gubbins/db/sql/my_pilot_db/db.py"
```

<!-- blacken-docs:on -->

Key points:

- Inherits from `BaseSQLDB`, which provides `self.conn`, transaction management, and the `metadata` class variable
- `get_available_ces()` uses a subquery to count active pilots per CE and filters by remaining capacity
- `submit_pilot()` creates a SUBMITTED record — the server-default handles timestamps
- `update_pilot_status()` explicitly sets `updated_at` on transitions

## Create the `__init__.py`

Create an empty `extensions/gubbins/gubbins-db/src/gubbins/db/sql/my_pilot_db/__init__.py` file.

## Register the entry point

Add the following to `extensions/gubbins/gubbins-db/pyproject.toml` under `[project.entry-points."diracx.dbs.sql"]`:

```toml
--8<-- "extensions/gubbins/gubbins-db/pyproject.toml:my_pilots_db_entry_point"
```

## Export from the package

Add the following to `extensions/gubbins/gubbins-db/src/gubbins/db/sql/__init__.py`:

<!-- blacken-docs:off -->

```python
--8<-- "extensions/gubbins/gubbins-db/src/gubbins/db/sql/__init__.py:my_pilots_db_init"
```

<!-- blacken-docs:on -->

## Add dependency injection

Add the following to `extensions/gubbins/gubbins-tasks/src/gubbins/tasks/depends.py`:

<!-- blacken-docs:off -->

```python
--8<-- "extensions/gubbins/gubbins-tasks/src/gubbins/tasks/depends.py:my_pilots_depends"
```

<!-- blacken-docs:on -->

This creates an `Annotated` type that FastAPI and the task worker can
resolve automatically — the `DBDepends` wrapper ensures the
transaction commits before the HTTP response is sent.
