"""DI type definitions for gubbins tasks.

Re-exported by ``gubbins.routers.dependencies`` so that both routers
and the task worker can resolve them.
"""

from __future__ import annotations

__all__ = ("LollygagDB",)

from typing import Annotated

from diracx.tasks.plumbing.depends import DBDepends

from gubbins.db.sql import LollygagDB as _LollygagDB

LollygagDB = Annotated[_LollygagDB, DBDepends(_LollygagDB.transaction)]
