from __future__ import annotations

import pytest

from noex_client import ClientOptions, NoexClient, NoexClientError
from tests.conftest import ServerInfo, start_test_server, stop_test_server

SESSIONS = {
    "admin": {"userId": "admin-1", "roles": ["admin"]},
    "writer": {"userId": "writer-1", "roles": ["writer"]},
    "reader": {"userId": "reader-1", "roles": ["reader"]},
}


@pytest.fixture
async def proc_server():
    info = await start_test_server(
        auth={"sessions": SESSIONS},
        buckets=[
            {
                "name": "orders",
                "schema": {
                    "status": {"type": "string"},
                    "total": {"type": "number"},
                },
            },
            {
                "name": "items",
                "schema": {
                    "orderId": {"type": "string", "required": True},
                    "price": {"type": "number", "required": True},
                },
            },
        ],
    )
    yield info
    await stop_test_server(info)


@pytest.fixture
async def proc_server_no_auth():
    info = await start_test_server(
        buckets=[
            {
                "name": "orders",
                "schema": {
                    "status": {"type": "string"},
                    "total": {"type": "number"},
                },
            },
        ],
    )
    yield info
    await stop_test_server(info)


def _make_client(url: str, token: str | None = None) -> NoexClient:
    from noex_client import AuthOptions

    if token:
        return NoexClient(url, ClientOptions(reconnect=False, auth=AuthOptions(token=token)))
    return NoexClient(url, ClientOptions(reconnect=False))


# ── register ─────────────────────────────────────────────────────────


async def test_register_procedure(proc_server: ServerInfo):
    client = _make_client(proc_server.url, "admin")
    await client.connect()

    result = await client.procedures.register({
        "name": "simple-proc",
        "description": "A simple procedure",
        "input": {"id": {"type": "string"}},
        "steps": [
            {"action": "store.get", "bucket": "orders", "key": "{{ input.id }}", "as": "order"},
        ],
    })

    assert result["name"] == "simple-proc"
    assert result["registered"] is True

    await client.disconnect()


async def test_register_duplicate(proc_server: ServerInfo):
    client = _make_client(proc_server.url, "admin")
    await client.connect()

    await client.procedures.register({
        "name": "dup-proc",
        "steps": [{"action": "store.get", "bucket": "orders", "key": "x", "as": "order"}],
    })

    with pytest.raises(NoexClientError) as exc_info:
        await client.procedures.register({
            "name": "dup-proc",
            "steps": [{"action": "store.get", "bucket": "orders", "key": "y", "as": "order"}],
        })
    assert exc_info.value.code == "ALREADY_EXISTS"

    await client.disconnect()


async def test_register_invalid(proc_server: ServerInfo):
    client = _make_client(proc_server.url, "admin")
    await client.connect()

    with pytest.raises(NoexClientError) as exc_info:
        await client.procedures.register({"name": "bad-proc", "steps": []})
    assert exc_info.value.code == "VALIDATION_ERROR"

    await client.disconnect()


# ── call ─────────────────────────────────────────────────────────────


async def test_call_simple_get(proc_server: ServerInfo):
    client = _make_client(proc_server.url, "admin")
    await client.connect()

    bucket = client.store.bucket("orders")
    inserted = await bucket.insert({"status": "pending", "total": 0})
    order_id = inserted["id"]

    await client.procedures.register({
        "name": "get-order",
        "input": {"orderId": {"type": "string"}},
        "steps": [
            {"action": "store.get", "bucket": "orders", "key": "{{ input.orderId }}", "as": "order"},
        ],
    })

    result = await client.procedures.call("get-order", {"orderId": order_id})

    assert result["success"] is True
    assert result["results"]["order"]["id"] == order_id
    assert result["results"]["order"]["status"] == "pending"

    await client.disconnect()


async def test_call_with_aggregation(proc_server: ServerInfo):
    client = _make_client(proc_server.url, "admin")
    await client.connect()

    order_bucket = client.store.bucket("orders")
    items_bucket = client.store.bucket("items")
    order = await order_bucket.insert({"status": "pending", "total": 0})
    order_id = order["id"]

    await items_bucket.insert({"orderId": order_id, "price": 100})
    await items_bucket.insert({"orderId": order_id, "price": 50})

    await client.procedures.register({
        "name": "calc-total",
        "input": {"orderId": {"type": "string"}},
        "steps": [
            {"action": "store.where", "bucket": "items", "filter": {"orderId": "{{ input.orderId }}"}, "as": "items"},
            {"action": "aggregate", "source": "items", "field": "price", "op": "sum", "as": "total"},
            {"action": "store.update", "bucket": "orders", "key": "{{ input.orderId }}", "data": {"total": "{{ total }}", "status": "calculated"}},
        ],
    })

    result = await client.procedures.call("calc-total", {"orderId": order_id})

    assert result["success"] is True
    assert result["results"]["total"] == 150

    updated = await order_bucket.get(order_id)
    assert updated["total"] == 150
    assert updated["status"] == "calculated"

    await client.disconnect()


async def test_call_not_found(proc_server: ServerInfo):
    client = _make_client(proc_server.url, "admin")
    await client.connect()

    with pytest.raises(NoexClientError) as exc_info:
        await client.procedures.call("ghost-procedure")
    assert exc_info.value.code == "NOT_FOUND"

    await client.disconnect()


# ── unregister ───────────────────────────────────────────────────────


async def test_unregister(proc_server: ServerInfo):
    client = _make_client(proc_server.url, "admin")
    await client.connect()

    await client.procedures.register({
        "name": "removable",
        "steps": [{"action": "store.get", "bucket": "orders", "key": "x", "as": "order"}],
    })

    result = await client.procedures.unregister("removable")
    assert result["name"] == "removable"
    assert result["unregistered"] is True

    with pytest.raises(NoexClientError) as exc_info:
        await client.procedures.call("removable")
    assert exc_info.value.code == "NOT_FOUND"

    await client.disconnect()


# ── update ───────────────────────────────────────────────────────────


async def test_update_procedure(proc_server: ServerInfo):
    client = _make_client(proc_server.url, "admin")
    await client.connect()

    await client.procedures.register({
        "name": "updatable",
        "description": "Original",
        "steps": [{"action": "store.get", "bucket": "orders", "key": "x", "as": "order"}],
    })

    result = await client.procedures.update("updatable", {"description": "Updated"})
    assert result["name"] == "updatable"
    assert result["updated"] is True

    detail = await client.procedures.get("updatable")
    assert detail["description"] == "Updated"

    await client.disconnect()


# ── get ──────────────────────────────────────────────────────────────


async def test_get_procedure(proc_server: ServerInfo):
    client = _make_client(proc_server.url, "admin")
    await client.connect()

    await client.procedures.register({
        "name": "detail-proc",
        "description": "Detailed",
        "input": {"id": {"type": "string"}},
        "steps": [{"action": "store.get", "bucket": "orders", "key": "{{ input.id }}", "as": "order"}],
    })

    detail = await client.procedures.get("detail-proc")

    assert detail["name"] == "detail-proc"
    assert detail["description"] == "Detailed"
    assert detail["input"] == {"id": {"type": "string"}}
    assert isinstance(detail["steps"], list)
    assert len(detail["steps"]) == 1

    await client.disconnect()


# ── list ─────────────────────────────────────────────────────────────


async def test_list_procedures(proc_server: ServerInfo):
    client = _make_client(proc_server.url, "admin")
    await client.connect()

    await client.procedures.register({
        "name": "proc-a",
        "steps": [{"action": "store.get", "bucket": "orders", "key": "x", "as": "order"}],
    })
    await client.procedures.register({
        "name": "proc-b",
        "steps": [
            {"action": "store.get", "bucket": "orders", "key": "x", "as": "o1"},
            {"action": "store.get", "bucket": "orders", "key": "y", "as": "o2"},
        ],
    })

    result = await client.procedures.list()

    assert len(result["procedures"]) == 2
    names = [p["name"] for p in result["procedures"]]
    assert "proc-a" in names
    assert "proc-b" in names

    proc_b = next(p for p in result["procedures"] if p["name"] == "proc-b")
    assert proc_b["stepsCount"] == 2

    await client.disconnect()


async def test_list_empty(proc_server: ServerInfo):
    client = _make_client(proc_server.url, "admin")
    await client.connect()

    result = await client.procedures.list()
    assert result["procedures"] == []

    await client.disconnect()


# ── Tier enforcement ─────────────────────────────────────────────────


async def test_reject_register_from_writer(proc_server: ServerInfo):
    client = _make_client(proc_server.url, "writer")
    await client.connect()

    with pytest.raises(NoexClientError) as exc_info:
        await client.procedures.register({
            "name": "forbidden",
            "steps": [{"action": "store.get", "bucket": "orders", "key": "x", "as": "o"}],
        })
    assert exc_info.value.code == "FORBIDDEN"

    await client.disconnect()


async def test_writer_can_call(proc_server: ServerInfo):
    admin = _make_client(proc_server.url, "admin")
    await admin.connect()
    await admin.procedures.register({
        "name": "callable",
        "steps": [{"action": "store.insert", "bucket": "orders", "data": {"status": "created", "total": 0}, "as": "order"}],
    })
    await admin.disconnect()

    writer = _make_client(proc_server.url, "writer")
    await writer.connect()
    result = await writer.procedures.call("callable")
    assert result["success"] is True
    await writer.disconnect()


async def test_reject_call_from_reader(proc_server: ServerInfo):
    admin = _make_client(proc_server.url, "admin")
    await admin.connect()
    await admin.procedures.register({
        "name": "not-for-readers",
        "steps": [{"action": "store.get", "bucket": "orders", "key": "x", "as": "o"}],
    })
    await admin.disconnect()

    reader = _make_client(proc_server.url, "reader")
    await reader.connect()

    with pytest.raises(NoexClientError) as exc_info:
        await reader.procedures.call("not-for-readers")
    assert exc_info.value.code == "FORBIDDEN"

    await reader.disconnect()


# ── No auth mode ─────────────────────────────────────────────────────


async def test_no_auth_mode(proc_server_no_auth: ServerInfo):
    client = _make_client(proc_server_no_auth.url)
    await client.connect()

    reg_result = await client.procedures.register({
        "name": "open-proc",
        "steps": [{"action": "store.insert", "bucket": "orders", "data": {"status": "open", "total": 0}, "as": "order"}],
    })
    assert reg_result["registered"] is True

    call_result = await client.procedures.call("open-proc")
    assert call_result["success"] is True

    list_result = await client.procedures.list()
    assert len(list_result["procedures"]) == 1

    await client.disconnect()
