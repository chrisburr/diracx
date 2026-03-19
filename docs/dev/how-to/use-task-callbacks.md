## Use task callbacks

The callback system provides a fan-out/fan-in pattern: spawn multiple child tasks and fire a callback task when all children complete.

### Basic usage

Use `spawn_with_callback` to schedule children and a callback together:

```python
from diracx.tasks.plumbing.callbacks import spawn_with_callback


class OrchestrationTask(BaseTask):
    async def execute(self, _redis, **kwargs):
        children = [
            SyncOwnersTask(owner_name="alice"),
            SyncOwnersTask(owner_name="bob"),
            SyncOwnersTask(owner_name="charlie"),
        ]
        callback = OwnerCleanupTask()

        group_id = await spawn_with_callback(children, callback, redis=_redis)
```

The children are scheduled immediately. When the last child completes, the worker automatically submits the callback task to the broker.

### How it works

1. `spawn_with_callback` generates a unique `group_id` and stores:
    - The serialized callback task in Redis
    - An atomic counter set to the number of children
2. Each child is scheduled with a `group_id` label
3. When a worker completes a child task that has a `group_id` label, it calls `on_child_complete` which:
    - Stores the child's result
    - Atomically decrements the remaining counter
4. When the counter reaches zero, the worker fires the callback

### Accessing `_redis` in `execute()`

The `_redis` parameter is injected by the worker when executing a task. To use it, include it as a keyword argument in `execute()`:

```python
class MyTask(BaseTask):
    async def execute(self, _redis, **kwargs):
        # _redis is a redis.asyncio.Redis connection
        ...
```

### Cleanup

All Redis keys created by the callback system are set with a TTL (default 24 hours). This means callback state is automatically cleaned up even if something goes wrong and the callback never fires.

The TTL can be configured via the `ttl_seconds` parameter:

```python
await spawn_with_callback(children, callback, redis=_redis, ttl_seconds=3600)  # 1 hour
```
