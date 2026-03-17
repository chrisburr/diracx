from __future__ import annotations

__all__ = ["LockedObjectType", "register_locked_object_type", "validate_registry"]

# String-based extensible registry for locked object types.
# Core types are registered here; extensions add their own via the
# ``diracx.lock_object_types`` entry-point group.

_REGISTRY: set[str] = set()


class LockedObjectType(str):
    """A validated locked-object type string.

    Acts like a plain ``str`` but raises ``ValueError`` at construction
    time if the value was not previously registered.
    """

    def __new__(cls, value: str):
        if value not in _REGISTRY:
            raise ValueError(
                f"Unknown LockedObjectType {value!r}. "
                f"Registered types: {sorted(_REGISTRY)}"
            )
        return super().__new__(cls, value)


def register_locked_object_type(name: str) -> str:
    """Register a new locked-object type and return the name."""
    _REGISTRY.add(name)
    return name


def validate_registry() -> None:
    """Validate that all entry-point-registered types are loaded.

    Called at startup to catch misconfigurations early.
    """
    from importlib.metadata import entry_points

    for ep in entry_points(group="diracx.lock_object_types"):
        ep.load()  # Side-effect: calls register_locked_object_type


# Register built-in types
TASK = register_locked_object_type("task")
JOB = register_locked_object_type("job")
TRANSFORMATION = register_locked_object_type("transformation")
