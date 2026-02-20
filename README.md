# noex-client

Python client SDK for [noex-server](https://github.com/hamicek/noex-server). Asyncio-native, 1:1 feature parity with the TypeScript client.

## Features

- **Store CRUD** with bucket API, cursor pagination, and aggregation
- **Reactive subscriptions** -- subscribe to server-side queries, receive push updates via callbacks
- **Transactions** -- atomic multi-bucket operations
- **Rules engine proxy** -- emit events, manage facts, subscribe to rule matches
- **Identity & auth** -- built-in user/role management, ACL, token and credential login
- **Audit & procedures** -- audit log queries, server-side procedure execution
- **Automatic reconnect** with exponential backoff, jitter, and subscription recovery
- **Heartbeat** -- automatic pong responses to server ping
- **Type-safe** -- full type hints, strict mypy, `TypedDict` for protocol structures
- **Minimal dependencies** -- only `websockets` (>=13.0)

## Installation

```bash
pip install noex-client
```

Requires Python >= 3.11.

## Quick Start

```python
import asyncio
from noex_client import NoexClient

async def main():
    client = NoexClient("ws://localhost:8080")
    await client.connect()

    # Store CRUD
    users = client.store.bucket("users")
    alice = await users.insert({"name": "Alice"})
    all_users = await users.all()

    # Reactive subscription
    unsub = await client.store.subscribe("all-users", lambda data: print("Updated:", data))

    # Rules
    await client.rules.emit("user.created", {"userId": alice["id"]})

    # Cleanup
    unsub()
    await client.disconnect()

asyncio.run(main())
```

### Context Manager

```python
async with NoexClient("ws://localhost:8080") as client:
    users = client.store.bucket("users")
    await users.insert({"name": "Alice"})
# Automatically disconnects
```

### Auth and Reconnect

```python
from noex_client import NoexClient, ClientOptions, AuthOptions, ReconnectOptions

client = NoexClient("ws://localhost:8080", ClientOptions(
    auth=AuthOptions(token="my-jwt-token"),
    reconnect=ReconnectOptions(
        max_retries=10,
        initial_delay_ms=500,
        max_delay_ms=15_000,
    ),
    request_timeout_ms=5_000,
))

client.on("reconnecting", lambda attempt: print(f"Reconnecting... attempt {attempt}"))
client.on("reconnected", lambda: print("Reconnected! Subscriptions restored."))

await client.connect()
```

When `auth.token` is set and the server requires authentication, the client automatically sends `auth.login` after connecting and after every reconnect.

---

## API

### NoexClient

#### `NoexClient(url, options=None)`

Creates a client instance. Does not open a connection -- call `connect()` to start.

```python
client = NoexClient("ws://localhost:8080", ClientOptions(
    auth=AuthOptions(token="jwt"),
    reconnect=True,
    request_timeout_ms=10_000,
    connect_timeout_ms=5_000,
    heartbeat=True,
))
```

#### `await client.connect() -> WelcomeInfo`

Opens the WebSocket connection and waits for the server welcome message. If auth is configured and the server requires authentication, login is performed automatically.

```python
welcome = await client.connect()
# WelcomeInfo(version='1.0.0', server_time=1706745600000, requires_auth=True)
```

#### `await client.disconnect() -> None`

Gracefully closes the connection. Rejects all pending requests, clears subscriptions, and stops any reconnect loop.

#### `client.state -> ConnectionState`

Current connection state: `"connecting"` | `"connected"` | `"reconnecting"` | `"disconnected"`.

#### `client.is_connected -> bool`

Shorthand for `client.state == "connected"`.

#### `client.on(event, handler) -> Unsubscribe`

Subscribe to client lifecycle events. Returns an unsubscribe function.

| Event | Handler signature | Description |
|-------|-------------------|-------------|
| `"connected"` | `() -> None` | Connection established (initial or reconnect) |
| `"disconnected"` | `(reason: str) -> None` | Connection lost or closed |
| `"reconnecting"` | `(attempt: int) -> None` | Reconnect attempt starting |
| `"reconnected"` | `() -> None` | Successfully reconnected |
| `"error"` | `(error: Exception) -> None` | Transport or reconnect error |
| `"welcome"` | `(info: WelcomeInfo) -> None` | Welcome message received from server |
| `"session_revoked"` | `() -> None` | Server revoked the current session |

---

### ClientOptions

```python
@dataclass(frozen=True)
class ClientOptions:
    auth: AuthOptions | None = None
    reconnect: bool | ReconnectOptions = True
    request_timeout_ms: int = 10_000
    connect_timeout_ms: int = 5_000
    heartbeat: bool = True
```

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `auth` | `AuthOptions` | `None` | Auth configuration for automatic login |
| `reconnect` | `bool \| ReconnectOptions` | `True` | Enable automatic reconnect with exponential backoff |
| `request_timeout_ms` | `int` | `10000` | Timeout for individual request/response round-trips |
| `connect_timeout_ms` | `int` | `5000` | Timeout for WebSocket connection and welcome message |
| `heartbeat` | `bool` | `True` | Automatically respond to server ping messages |

#### AuthOptions

```python
@dataclass(frozen=True)
class AuthOptions:
    token: str | None = None                     # Token for auth.login
    credentials: CredentialOptions | None = None  # Username/password for identity.login
```

#### ReconnectOptions

```python
@dataclass(frozen=True)
class ReconnectOptions:
    max_retries: float = float("inf")
    initial_delay_ms: int = 1_000
    max_delay_ms: int = 30_000
    backoff_multiplier: float = 2.0
    jitter_ms: int = 500
```

---

### StoreAPI

Access via `client.store`.

#### `store.bucket(name) -> BucketAPI`

Returns a `BucketAPI` handle for the named bucket. Does not make a request -- the bucket handle is a thin wrapper that attaches the bucket name to each operation.

```python
users = client.store.bucket("users")
```

#### `await store.subscribe(query, callback, params=None) -> Unsubscribe`

Subscribe to a reactive server-side query. The callback receives the initial data immediately and is called again whenever the query result changes on the server.

```python
unsub = await client.store.subscribe("all-users", lambda users: print("Users:", users))

# With parameters
unsub = await client.store.subscribe(
    "users-by-role",
    lambda admins: print("Admins:", admins),
    params={"role": "admin"},
)

# Unsubscribe (synchronous)
unsub()
```

Subscriptions survive reconnect -- after a successful reconnect the client automatically resubscribes and delivers fresh data to the callback.

#### `await store.unsubscribe(subscription_id) -> None`

Cancel a subscription by its server-assigned ID.

#### `await store.transaction(operations) -> dict`

Execute multiple store operations atomically.

```python
result = await client.store.transaction([
    {"op": "get", "bucket": "users", "key": "user-1"},
    {"op": "update", "bucket": "users", "key": "user-1", "data": {"credits": 400}},
    {"op": "insert", "bucket": "logs", "data": {"action": "credit_update"}},
])
```

Supported ops: `get`, `insert`, `update`, `delete`, `where`, `findOne`, `count`.

#### Admin -- Bucket Management

```python
await store.define_bucket("users", {"schema": {"name": {"type": "string"}}})
await store.update_bucket("users", {"schema": {"email": {"type": "string"}}})
schema = await store.get_bucket_schema("users")
await store.drop_bucket("users")
```

#### Admin -- Query Management

```python
await store.define_query("all-users", {"type": "all", "bucket": "users"})
queries = await store.list_queries()
await store.undefine_query("all-users")
```

#### Metadata

```python
buckets = await store.buckets()
stats = await store.stats()
```

---

### BucketAPI

Access via `client.store.bucket(name)`.

#### CRUD

| Method | Returns |
|--------|---------|
| `await bucket.insert(data)` | `dict` -- inserted record with metadata |
| `await bucket.get(key)` | `dict \| None` |
| `await bucket.update(key, data)` | `dict` -- updated record |
| `await bucket.delete(key)` | `None` |

#### Queries

| Method | Returns |
|--------|---------|
| `await bucket.all()` | `list[dict]` |
| `await bucket.where(filter)` | `list[dict]` |
| `await bucket.find_one(filter)` | `dict \| None` |
| `await bucket.count(filter=None)` | `int` |
| `await bucket.first(n)` | `list[dict]` |
| `await bucket.last(n)` | `list[dict]` |
| `await bucket.paginate(limit=..., after=...)` | `dict` -- paginated result |

#### Aggregation

| Method | Returns |
|--------|---------|
| `await bucket.sum(field, filter=None)` | `float` |
| `await bucket.avg(field, filter=None)` | `float` |
| `await bucket.min(field, filter=None)` | `float \| None` |
| `await bucket.max(field, filter=None)` | `float \| None` |

#### Bulk

| Method | Description |
|--------|-------------|
| `await bucket.clear()` | Remove all records from the bucket |

---

### RulesAPI

Access via `client.rules`.

#### Events

```python
event = await client.rules.emit("user.created", {"userId": "123"})
# {'id': '...', 'topic': 'user.created', 'data': {...}, 'timestamp': ...}

# With correlation/causation IDs
event = await client.rules.emit(
    "order.completed",
    {"orderId": "456"},
    correlation_id="corr-1",
    causation_id="cause-1",
)
```

#### Facts

```python
await client.rules.set_fact("user:1:status", "active")
status = await client.rules.get_fact("user:1:status")
deleted = await client.rules.delete_fact("user:1:status")
facts = await client.rules.query_facts("user:*:status")
all_facts = await client.rules.get_all_facts()
```

#### Subscriptions

Subscribe to real-time rule events by topic pattern:

```python
unsub = await client.rules.subscribe("user.*", lambda event, topic: print(f"{topic}: {event}"))

unsub()
```

#### Admin

```python
await client.rules.register_rule({"id": "my-rule", "when": {...}, "then": {...}})
await client.rules.enable_rule("my-rule")
await client.rules.disable_rule("my-rule")
rule = await client.rules.get_rule("my-rule")
rules = await client.rules.get_rules()
await client.rules.update_rule("my-rule", {"then": {...}})
validation = await client.rules.validate_rule({...})
await client.rules.unregister_rule("my-rule")
```

#### Stats

```python
stats = await client.rules.stats()
# {'rulesCount': ..., 'factsCount': ..., 'eventsProcessed': ...}
```

---

### AuthAPI

Access via `client.auth`.

```python
session = await client.auth.login("jwt-token")
# {'userId': '...', 'roles': [...], 'expiresAt': ...}

current = await client.auth.whoami()
await client.auth.logout()
```

When `auth.token` is set in `ClientOptions`, login is performed automatically after connect and after each reconnect.

---

### IdentityAPI

Access via `client.identity`. Built-in user management with roles and ACL.

#### Auth

```python
result = await client.identity.login("admin", "password")
result = await client.identity.login_with_secret("admin-secret")
me = await client.identity.whoami()
session = await client.identity.refresh_session()
await client.identity.logout()
```

When `auth.credentials` is set in `ClientOptions`, credential login is performed automatically after connect and after each reconnect.

#### User Management

```python
user = await client.identity.create_user({"username": "alice", "password": "s3cret"})
user = await client.identity.get_user(user_id)
await client.identity.update_user(user_id, {"displayName": "Alice"})
users = await client.identity.list_users(page=1, page_size=20)
await client.identity.enable_user(user_id)
await client.identity.disable_user(user_id)
await client.identity.delete_user(user_id)
```

#### Password

```python
await client.identity.change_password(user_id, "old-pass", "new-pass")
await client.identity.reset_password(user_id, "new-pass")
```

#### Roles

```python
role = await client.identity.create_role({"name": "editor", "permissions": [...]})
await client.identity.assign_role(user_id, "editor")
roles = await client.identity.get_user_roles(user_id)
await client.identity.remove_role(user_id, "editor")
all_roles = await client.identity.list_roles()
await client.identity.update_role(role_id, {"permissions": [...]})
await client.identity.delete_role(role_id)
```

#### ACL

```python
await client.identity.grant({"userId": user_id, "resource": "bucket:users", "permission": "read"})
await client.identity.revoke({"userId": user_id, "resource": "bucket:users", "permission": "read"})
acl = await client.identity.get_acl("bucket", "users")
access = await client.identity.my_access()
```

#### Ownership

```python
owner = await client.identity.get_owner("bucket", "users")
await client.identity.transfer_owner("bucket", "users", new_owner_id)
```

---

### AuditAPI

Access via `client.audit`.

```python
entries = await client.audit.query({"userId": "admin-1", "limit": 50})
```

Supported filter keys: `userId`, `operation`, `result`, `from`, `to`, `limit`.

---

### ProceduresAPI

Access via `client.procedures`.

```python
# Register
await client.procedures.register({"name": "calculate-total", "steps": [...]})

# Execute
result = await client.procedures.call("calculate-total", {"orderId": "123"})

# Admin
proc = await client.procedures.get("calculate-total")
all_procs = await client.procedures.list()
await client.procedures.update("calculate-total", {"steps": [...]})
await client.procedures.unregister("calculate-total")
```

---

## Error Handling

All errors from the server are propagated as `NoexClientError` with a machine-readable `code`:

```python
from noex_client import NoexClientError, RequestTimeoutError, DisconnectedError

try:
    await client.store.bucket("users").insert({"name": ""})
except NoexClientError as e:
    match e.code:
        case "VALIDATION_ERROR":
            print(f"Validation failed: {e.details}")
        case "UNAUTHORIZED":
            print("Need to login first")
        case "NOT_FOUND":
            print("Resource not found")
```

| Error class | Code | Description |
|-------------|------|-------------|
| `NoexClientError` | *(server code)* | Base class for all server errors |
| `RequestTimeoutError` | `TIMEOUT` | Request did not receive a response within `request_timeout_ms` |
| `DisconnectedError` | `DISCONNECTED` | Attempted to send while not connected, or connection was lost |

Pending requests at the time of a disconnect are rejected with `DisconnectedError`. They are **not** retried automatically -- the server does not persist request state across connections and automatic retry of non-idempotent operations (insert, emit) could cause duplicates.

---

## Reconnect Behavior

Reconnect is enabled by default. When the connection drops unexpectedly:

1. All pending requests are rejected with `DisconnectedError`
2. The client enters `"reconnecting"` state and emits `reconnecting` events
3. Exponential backoff with jitter determines the delay between attempts
4. On successful reconnect:
   - Auto-login is performed (if configured)
   - All active subscriptions are restored with fresh data
   - `"reconnected"` event is emitted
5. If max retries are exhausted, the client enters `"disconnected"` state

Calling `disconnect()` at any point stops the reconnect loop immediately.

---

## Production

In production, always use `wss://` (WebSocket over TLS) instead of plain `ws://`. The noex-server itself does not terminate TLS -- place it behind a reverse proxy (nginx, Caddy) that handles TLS and forwards traffic to the server.

```python
client = NoexClient("wss://api.example.com")
```

---

## License

MIT
