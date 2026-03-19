# Part 4: Router

We add a minimal router with two endpoints for manual interaction
with the pilot system.

## Implementation

Create `extensions/gubbins/gubbins-routers/src/gubbins/routers/my_pilots.py`:

<!-- blacken-docs:off -->

```python
--8<-- "extensions/gubbins/gubbins-routers/src/gubbins/routers/my_pilots.py"
```

<!-- blacken-docs:on -->

Key points:

- **Access policy**: `MyPilotsAccessPolicy` inherits from
    `BaseAccessPolicy`. The `policy` method is a `@staticmethod` with
    positional-only args `(policy_name, user_info)` and keyword-only
    args with defaults. Here we allow all authenticated users — in a
    real system you'd check properties.
- **DB dependency**: `MyPilotDB = Annotated[_MyPilotDB, Depends(_MyPilotDB.transaction)]`
    creates a per-request transaction that auto-commits on success.
- **POST `/submit/{ce_name}`**: Directly inserts a pilot submission
    into the database (bypassing the task system for manual use).
- **GET `/summary`**: Returns pilot counts grouped by status.

## Register entry points

Add to `extensions/gubbins/gubbins-routers/pyproject.toml`:

Service entry point:

```toml
--8<-- "extensions/gubbins/gubbins-routers/pyproject.toml:my_pilots_service_entry_point"
```

Access policy entry point:

```toml
--8<-- "extensions/gubbins/gubbins-routers/pyproject.toml:my_pilots_access_policy_entry_point"
```

## Update package exports

Re-export the DB dependency in `extensions/gubbins/gubbins-routers/src/gubbins/routers/dependencies.py`:

<!-- blacken-docs:off -->

```python
--8<-- "extensions/gubbins/gubbins-routers/src/gubbins/routers/dependencies.py:my_pilots_router_depends"
```

<!-- blacken-docs:on -->
