"""Expression helper functions for building JSON expression trees.

Each function returns a plain dict with a ``$operator`` key — no logic,
just syntactic sugar over the wire format.

Usage::

    from noex_client.logic import expr as e

    total = e.multiply('$qty', '$price')
    discount = e.cond(e.gt('$discount', 0), e.subtract('$total', '$discount'), '$total')
"""

from __future__ import annotations

from typing import Any

# Type alias matching the TS ``Expression`` union.
Arg = Any


class _Expr:
    """Namespace object that generates JSON expression operators."""

    __slots__ = ()

    # ── Arithmetic ──────────────────────────────────────────────────

    @staticmethod
    def add(a: Arg, b: Arg) -> dict[str, Any]:
        return {"$add": [a, b]}

    @staticmethod
    def subtract(a: Arg, b: Arg) -> dict[str, Any]:
        return {"$subtract": [a, b]}

    @staticmethod
    def multiply(a: Arg, b: Arg) -> dict[str, Any]:
        return {"$multiply": [a, b]}

    @staticmethod
    def divide(a: Arg, b: Arg) -> dict[str, Any]:
        return {"$divide": [a, b]}

    @staticmethod
    def mod(a: Arg, b: Arg) -> dict[str, Any]:
        return {"$mod": [a, b]}

    @staticmethod
    def abs(a: Arg) -> dict[str, Any]:
        return {"$abs": a}

    @staticmethod
    def round(a: Arg, decimals: int = 0) -> dict[str, Any]:
        return {"$round": [a, decimals]}

    @staticmethod
    def floor(a: Arg) -> dict[str, Any]:
        return {"$floor": a}

    @staticmethod
    def ceil(a: Arg) -> dict[str, Any]:
        return {"$ceil": a}

    # ── Comparison ──────────────────────────────────────────────────

    @staticmethod
    def eq(a: Arg, b: Arg) -> dict[str, Any]:
        return {"$eq": [a, b]}

    @staticmethod
    def neq(a: Arg, b: Arg) -> dict[str, Any]:
        return {"$neq": [a, b]}

    @staticmethod
    def gt(a: Arg, b: Arg) -> dict[str, Any]:
        return {"$gt": [a, b]}

    @staticmethod
    def gte(a: Arg, b: Arg) -> dict[str, Any]:
        return {"$gte": [a, b]}

    @staticmethod
    def lt(a: Arg, b: Arg) -> dict[str, Any]:
        return {"$lt": [a, b]}

    @staticmethod
    def lte(a: Arg, b: Arg) -> dict[str, Any]:
        return {"$lte": [a, b]}

    @staticmethod
    def between(a: Arg, min_val: Arg, max_val: Arg) -> dict[str, Any]:
        return {"$between": [a, min_val, max_val]}

    @staticmethod
    def is_in(a: Arg, values: list[Arg]) -> dict[str, Any]:
        return {"$in": [a, values]}

    # ── Logical ─────────────────────────────────────────────────────

    @staticmethod
    def and_(*conds: Arg) -> dict[str, Any]:
        return {"$and": list(conds)}

    @staticmethod
    def or_(*conds: Arg) -> dict[str, Any]:
        return {"$or": list(conds)}

    @staticmethod
    def not_(a: Arg) -> dict[str, Any]:
        return {"$not": a}

    @staticmethod
    def cond(condition: Arg, then: Arg, otherwise: Arg) -> dict[str, Any]:
        return {"$cond": [condition, then, otherwise]}

    # ── String ──────────────────────────────────────────────────────

    @staticmethod
    def concat(*parts: Arg) -> dict[str, Any]:
        return {"$concat": list(parts)}

    @staticmethod
    def upper(a: Arg) -> dict[str, Any]:
        return {"$upper": a}

    @staticmethod
    def lower(a: Arg) -> dict[str, Any]:
        return {"$lower": a}

    @staticmethod
    def length(a: Arg) -> dict[str, Any]:
        return {"$length": a}

    @staticmethod
    def trim(a: Arg) -> dict[str, Any]:
        return {"$trim": a}

    @staticmethod
    def substring(a: Arg, start: int, length: int | None = None) -> dict[str, Any]:
        if length is not None:
            return {"$substring": [a, start, length]}
        return {"$substring": [a, start]}

    # ── Date ────────────────────────────────────────────────────────

    @staticmethod
    def now() -> dict[str, bool]:
        return {"$now": True}

    @staticmethod
    def year(a: Arg) -> dict[str, Any]:
        return {"$year": a}

    @staticmethod
    def month(a: Arg) -> dict[str, Any]:
        return {"$month": a}

    @staticmethod
    def day(a: Arg) -> dict[str, Any]:
        return {"$day": a}

    @staticmethod
    def days_between(a: Arg, b: Arg) -> dict[str, Any]:
        return {"$daysBetween": [a, b]}

    @staticmethod
    def date_add(date: Arg, n: int, unit: str) -> dict[str, Any]:
        return {"$dateAdd": [date, n, unit]}

    # ── Aggregate ───────────────────────────────────────────────────

    @staticmethod
    def sum(field: str) -> dict[str, str]:
        return {"$sum": field}

    @staticmethod
    def avg(field: str) -> dict[str, str]:
        return {"$avg": field}

    @staticmethod
    def min(field: str) -> dict[str, str]:
        return {"$min": field}

    @staticmethod
    def max(field: str) -> dict[str, str]:
        return {"$max": field}

    @staticmethod
    def count(field: str | None = None) -> dict[str, str]:
        return {"$count": field if field is not None else "*"}

    # ── Utility ─────────────────────────────────────────────────────

    @staticmethod
    def f(field_name: str) -> str:
        """Shorthand for field reference: ``expr.f('price')`` → ``'$price'``."""
        return f"${field_name}"


expr = _Expr()
