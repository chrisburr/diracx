# Part 6: Running locally

!!! note "Work in progress"

    Full local execution (with worker, scheduler, and Redis) requires
    extending `run_local.sh`. This is planned for a future update.

## Interactive execution

You can run individual tasks interactively using the CLI:

```bash
diracx-task-run call my_pilots:MyPilotTask reliable-ce.example.org
```

This bypasses the broker and executes the task directly, which is
useful for development and debugging.

## What's next

- Read the [Tasks explanation](../../explanations/tasks/index.md) for
    deeper understanding of the broker lifecycle
- Check the [admin tasks guide](../../../admin/how-to/tasks/index.md)
    for operational guidance
