from __future__ import annotations

from typing import Any, Awaitable, Callable

SendFn = Callable[[str, dict[str, Any] | None], Awaitable[Any]]


class IdentityAPI:
    """Built-in identity management — users, roles, ACL, ownership."""

    def __init__(self, send: SendFn) -> None:
        self._send = send

    # -- Auth -------------------------------------------------------------

    async def login(self, username: str, password: str) -> dict[str, Any]:
        result: dict[str, Any] = await self._send(
            "identity.login", {"username": username, "password": password}
        )
        return result

    async def login_with_secret(self, secret: str) -> dict[str, Any]:
        result: dict[str, Any] = await self._send(
            "identity.loginWithSecret", {"secret": secret}
        )
        return result

    async def logout(self) -> None:
        await self._send("identity.logout", {})

    async def whoami(self) -> dict[str, Any]:
        result: dict[str, Any] = await self._send("identity.whoami", {})
        return result

    async def refresh_session(self) -> dict[str, Any]:
        result: dict[str, Any] = await self._send(
            "identity.refreshSession", {}
        )
        return result

    # -- User Management --------------------------------------------------

    async def create_user(self, input: dict[str, Any]) -> dict[str, Any]:
        result: dict[str, Any] = await self._send(
            "identity.createUser", input
        )
        return result

    async def get_user(self, user_id: str) -> dict[str, Any]:
        result: dict[str, Any] = await self._send(
            "identity.getUser", {"userId": user_id}
        )
        return result

    async def update_user(
        self, user_id: str, updates: dict[str, Any]
    ) -> dict[str, Any]:
        payload: dict[str, Any] = {"userId": user_id}
        for key in ("displayName", "email", "metadata"):
            if key in updates:
                payload[key] = updates[key]
        result: dict[str, Any] = await self._send(
            "identity.updateUser", payload
        )
        return result

    async def delete_user(self, user_id: str) -> None:
        await self._send("identity.deleteUser", {"userId": user_id})

    async def list_users(
        self, *, page: int = 1, page_size: int = 20
    ) -> dict[str, Any]:
        result: dict[str, Any] = await self._send(
            "identity.listUsers", {"page": page, "pageSize": page_size}
        )
        return result

    async def enable_user(self, user_id: str) -> dict[str, Any]:
        result: dict[str, Any] = await self._send(
            "identity.enableUser", {"userId": user_id}
        )
        return result

    async def disable_user(self, user_id: str) -> dict[str, Any]:
        result: dict[str, Any] = await self._send(
            "identity.disableUser", {"userId": user_id}
        )
        return result

    # -- Password ---------------------------------------------------------

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

    # -- Roles ------------------------------------------------------------

    async def create_role(self, input: dict[str, Any]) -> dict[str, Any]:
        result: dict[str, Any] = await self._send(
            "identity.createRole", input
        )
        return result

    async def update_role(
        self, role_id: str, updates: dict[str, Any]
    ) -> dict[str, Any]:
        payload: dict[str, Any] = {"roleId": role_id}
        for key in ("description", "permissions"):
            if key in updates:
                payload[key] = updates[key]
        result: dict[str, Any] = await self._send(
            "identity.updateRole", payload
        )
        return result

    async def delete_role(self, role_id: str) -> None:
        await self._send("identity.deleteRole", {"roleId": role_id})

    async def list_roles(self) -> list[dict[str, Any]]:
        result = await self._send("identity.listRoles", {})
        roles: list[dict[str, Any]] = result["roles"]
        return roles

    async def assign_role(self, user_id: str, role_name: str) -> None:
        await self._send(
            "identity.assignRole", {"userId": user_id, "roleName": role_name}
        )

    async def remove_role(self, user_id: str, role_name: str) -> None:
        await self._send(
            "identity.removeRole", {"userId": user_id, "roleName": role_name}
        )

    async def get_user_roles(self, user_id: str) -> list[dict[str, Any]]:
        result = await self._send(
            "identity.getUserRoles", {"userId": user_id}
        )
        roles: list[dict[str, Any]] = result["roles"]
        return roles

    # -- ACL --------------------------------------------------------------

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
        entries: list[dict[str, Any]] = result["entries"]
        return entries

    async def my_access(self) -> dict[str, Any]:
        result: dict[str, Any] = await self._send("identity.myAccess", {})
        return result

    # -- Ownership --------------------------------------------------------

    async def get_owner(
        self, resource_type: str, resource_name: str
    ) -> dict[str, Any] | None:
        result = await self._send(
            "identity.getOwner",
            {"resourceType": resource_type, "resourceName": resource_name},
        )
        owner: dict[str, Any] | None = result.get("owner")
        return owner

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
