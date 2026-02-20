# Changelog

## 0.1.0 (2025-02-20)

Initial release.

- Store CRUD with bucket API, cursor pagination, and aggregation
- Reactive subscriptions with push updates via callbacks
- Atomic transactions (multi-bucket)
- Rules engine proxy (events, facts, subscriptions, admin)
- Identity & auth (users, roles, ACL, token and credential login)
- Audit log queries
- Server-side procedures
- Automatic reconnect with exponential backoff, jitter, and subscription recovery
- Heartbeat (automatic pong responses)
- Full type hints, strict mypy, `py.typed` marker (PEP 561)
- Async context manager (`async with NoexClient(...) as client:`)
