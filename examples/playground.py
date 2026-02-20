import asyncio
from noex_client import NoexClient

async def main():
    async with NoexClient("ws://127.0.0.1:4000") as client:
        # Definovat bucket
        await client.store.define_bucket("users", {
            "key": "id",
            "schema": {
                "id": {"type": "string", "generated": "uuid"},
                "name": {"type": "string", "required": True},
                "age": {"type": "number"},
            },
        })

        users = client.store.bucket("users")

        # CRUD
        alice = await users.insert({"name": "Alice", "age": 30})
        print("Inserted:", alice)

        await users.update(alice["id"], {"age": 31})
        print("Updated:", await users.get(alice["id"]))

        await users.insert({"name": "Bob", "age": 25})
        print("All:", await users.all())
        print("Count:", await users.count())
        print("Avg age:", await users.avg("age"))

        # Reaktivní subscription
        await client.store.define_query("all-users", {
            "bucket": "users",
            "type": "all",
        })

        def on_change(data):
            print(">>> Push:", data)

        unsub = await client.store.subscribe("all-users", on_change)

        await users.insert({"name": "Charlie", "age": 40})
        await asyncio.sleep(0.1)  # počkat na push

        unsub()

        # Rules
        await client.rules.set_fact("app:version", "1.0")
        print("Fact:", await client.rules.get_fact("app:version"))

        event = await client.rules.emit("user.created", {"name": "Alice"})
        print("Event:", event)

asyncio.run(main())
