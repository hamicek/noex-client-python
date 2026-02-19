from __future__ import annotations

from typing import Any, Awaitable, Callable

SendFn = Callable[[str, dict[str, Any] | None], Awaitable[Any]]


class IdentityAPI:
    """Built-in identity management — users, roles, ACL, ownership."""

    def __init__(self, send: SendFn) -> None:
        self._send = send

    # ── Auth ──────────────────────────────────────────────────────

    async def login(self, username: str, password: str) -> dict[str, Any]:
        return await self._send("identity.login", {"username": username, "password": password})

    async def login_with_secret(self, secret: str) -> dict[str, Any]:
        return await self._send("identity.loginWithSecret", {"secret": secret})

    async def logout(self) -> None:
        await self._send("identity.logout", {})

    async def whoami(self) -> dict[str, Any]:
        return await self._send("identity.whoami", {})

    async def refresh_session(self) -> dict[str, Any]:
        return await self._send("identity.refreshSession", {})

    # ── User Management ──────────────────────────────────────────

    async def create_user(self, input: dict[str, Any]) -> dict[str, Any]:
        return await self._send("identity.createUser", input)

    async def get_user(self, user_id: str) -> dict[str, Any]:
        return await self._send("identity.getUser", {"userId": user_id})

    async def update_user(self, user_id: str, updates: dict[str, Any]) -> dict[str, Any]:
        payload: dict[str, Any] = {"userId": user_id}
        for key in ("displayName", "email", "metadata"):
            if key in updates:
                payload[key] = updates[key]
        return await self._send("identity.updateUser", payload)

    async def delete_user(self, user_id: str) -> None:
        await self._send("identity.deleteUser", {"userId": user_id})

    async def list_users(
        self, *, page: int = 1, page_size: int = 20
    ) -> dict[str, Any]:
        return await self._send(
            "identity.listUsers", {"page": page, "pageSize": page_size}
        )

    async def enable_user(self, user_id: str) -> dict[str, Any]:
        return await self._send("identity.enableUser", {"userId": user_id})

    async def disable_user(self, user_id: str) -> dict[str, Any]:
        return await self._send("identity.disableUser", {"userId": user_id})

    # ── Password ─────────────────────────────────────────────────

    async def change_password(
        self, user_id: str, current_password: str, new_password: str
    ) -> None:
        await self._send(
            "identity.changePassword",
            {
                "userId": user_id,
                "currentPassword": current_password,
                "newPassword": new_password,
            },
        )

    async def reset_password(self, user_id: str, new_password: str) -> None:
        await self._send(
            "identity.resetPassword",
            {"userId": user_id, "newPassword": new_password},
        )

    # ── Roles ────────────────────────────────────────────────────

    async def create_role(self, input: dict[str, Any]) -> dict[str, Any]:
        return await self._send("identity.createRole", input)

    async def update_role(self, role_id: str, updates: dict[str, Any]) -> dict[str, Any]:
        payload: dict[str, Any] = {"roleId": role_id}
        for key in ("description", "permissions"):
            if key in updates:
                payload[key] = updates[key]
        return await self._send("identity.updateRole", payload)

    async def delete_role(self, role_id: str) -> None:
        await self._send("identity.deleteRole", {"roleId": role_id})

    async def list_roles(self) -> list[dict[str, Any]]:
        result = await self._send("identity.listRoles", {})
        return result["roles"]

    async def assign_role(self, user_id: str, role_name: str) -> None:
        await self._send(
            "identity.assignRole", {"userId": user_id, "roleName": role_name}
        )

    async def remove_role(self, user_id: str, role_name: str) -> None:
        await self._send(
            "identity.removeRole", {"userId": user_id, "roleName": role_name}
        )

    async def get_user_roles(self, user_id: str) -> list[dict[str, Any]]:
        result = await self._send("identity.getUserRoles", {"userId": user_id})
        return result["roles"]

    # ── ACL ──────────────────────────────────────────────────────

    async def grant(self, input: dict[str, Any]) -> None:
        await self._send("identity.grant", input)

    async def revoke(self, input: dict[str, Any]) -> None:
        await self._send("identity.revoke", input)

    async def get_acl(
        self, resource_type: str, resource_name: str
    ) -> list[dict[str, Any]]:
        result = await self._send(
            "identity.getAcl",
            {"resourceType": resource_type, "resourceName": resource_name},
        )
        return result["entries"]

    async def my_access(self) -> dict[str, Any]:
        return await self._send("identity.myAccess", {})

    # ── Ownership ────────────────────────────────────────────────

    async def get_owner(
        self, resource_type: str, resource_name: str
    ) -> dict[str, Any] | None:
        result = await self._send(
            "identity.getOwner",
            {"resourceType": resource_type, "resourceName": resource_name},
        )
        return result.get("owner")

    async def transfer_owner(
        self, resource_type: str, resource_name: str, new_owner_id: str
    ) -> None:
        await self._send(
            "identity.transferOwner",
            {
                "resourceType": resource_type,
                "resourceName": resource_name,
                "newOwnerId": new_owner_id,
            },
        )
