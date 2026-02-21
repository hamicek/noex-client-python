"""Unit tests for expression helpers — verify each helper produces the correct JSON."""

from __future__ import annotations

from noex_client.logic.expr import expr as e


# ── Arithmetic ──────────────────────────────────────────────────────


class TestArithmetic:
    def test_add(self) -> None:
        assert e.add("$a", "$b") == {"$add": ["$a", "$b"]}

    def test_add_with_literal(self) -> None:
        assert e.add("$price", 10) == {"$add": ["$price", 10]}

    def test_subtract(self) -> None:
        assert e.subtract("$total", "$discount") == {"$subtract": ["$total", "$discount"]}

    def test_multiply(self) -> None:
        assert e.multiply("$qty", "$price") == {"$multiply": ["$qty", "$price"]}

    def test_divide(self) -> None:
        assert e.divide("$total", "$count") == {"$divide": ["$total", "$count"]}

    def test_mod(self) -> None:
        assert e.mod("$value", 2) == {"$mod": ["$value", 2]}

    def test_abs(self) -> None:
        assert e.abs("$balance") == {"$abs": "$balance"}

    def test_round_default(self) -> None:
        assert e.round("$price") == {"$round": ["$price", 0]}

    def test_round_with_decimals(self) -> None:
        assert e.round("$price", 2) == {"$round": ["$price", 2]}

    def test_floor(self) -> None:
        assert e.floor("$avg") == {"$floor": "$avg"}

    def test_ceil(self) -> None:
        assert e.ceil("$avg") == {"$ceil": "$avg"}


# ── Comparison ──────────────────────────────────────────────────────


class TestComparison:
    def test_eq(self) -> None:
        assert e.eq("$status", "paid") == {"$eq": ["$status", "paid"]}

    def test_neq(self) -> None:
        assert e.neq("$status", "cancelled") == {"$neq": ["$status", "cancelled"]}

    def test_gt(self) -> None:
        assert e.gt("$amount", 1000) == {"$gt": ["$amount", 1000]}

    def test_gte(self) -> None:
        assert e.gte("$age", 18) == {"$gte": ["$age", 18]}

    def test_lt(self) -> None:
        assert e.lt("$stock", 10) == {"$lt": ["$stock", 10]}

    def test_lte(self) -> None:
        assert e.lte("$price", 500) == {"$lte": ["$price", 500]}

    def test_between(self) -> None:
        assert e.between("$age", 18, 65) == {"$between": ["$age", 18, 65]}

    def test_is_in(self) -> None:
        assert e.is_in("$status", ["paid", "shipped"]) == {
            "$in": ["$status", ["paid", "shipped"]]
        }


# ── Logical ─────────────────────────────────────────────────────────


class TestLogical:
    def test_and(self) -> None:
        cond1 = e.gt("$a", 0)
        cond2 = e.lt("$b", 100)
        result = e.and_(cond1, cond2)
        assert result == {"$and": [{"$gt": ["$a", 0]}, {"$lt": ["$b", 100]}]}

    def test_and_single(self) -> None:
        assert e.and_(e.gt("$a", 0)) == {"$and": [{"$gt": ["$a", 0]}]}

    def test_or(self) -> None:
        result = e.or_(e.eq("$x", 1), e.eq("$x", 2))
        assert result == {"$or": [{"$eq": ["$x", 1]}, {"$eq": ["$x", 2]}]}

    def test_not(self) -> None:
        result = e.not_(e.eq("$status", "cancelled"))
        assert result == {"$not": {"$eq": ["$status", "cancelled"]}}

    def test_cond(self) -> None:
        result = e.cond(e.gt("$a", 0), "$a", 0)
        assert result == {"$cond": [{"$gt": ["$a", 0]}, "$a", 0]}


# ── String ──────────────────────────────────────────────────────────


class TestString:
    def test_concat(self) -> None:
        assert e.concat("$first", " ", "$last") == {
            "$concat": ["$first", " ", "$last"]
        }

    def test_upper(self) -> None:
        assert e.upper("$name") == {"$upper": "$name"}

    def test_lower(self) -> None:
        assert e.lower("$email") == {"$lower": "$email"}

    def test_length(self) -> None:
        assert e.length("$name") == {"$length": "$name"}

    def test_trim(self) -> None:
        assert e.trim("$input") == {"$trim": "$input"}

    def test_substring_without_length(self) -> None:
        assert e.substring("$code", 0) == {"$substring": ["$code", 0]}

    def test_substring_with_length(self) -> None:
        assert e.substring("$code", 0, 3) == {"$substring": ["$code", 0, 3]}


# ── Date ────────────────────────────────────────────────────────────


class TestDate:
    def test_now(self) -> None:
        assert e.now() == {"$now": True}

    def test_year(self) -> None:
        assert e.year("$createdAt") == {"$year": "$createdAt"}

    def test_month(self) -> None:
        assert e.month("$createdAt") == {"$month": "$createdAt"}

    def test_day(self) -> None:
        assert e.day("$createdAt") == {"$day": "$createdAt"}

    def test_days_between(self) -> None:
        assert e.days_between("$issued", "$due") == {
            "$daysBetween": ["$issued", "$due"]
        }

    def test_date_add(self) -> None:
        assert e.date_add("$issued", 30, "day") == {
            "$dateAdd": ["$issued", 30, "day"]
        }


# ── Aggregate ───────────────────────────────────────────────────────


class TestAggregate:
    def test_sum(self) -> None:
        assert e.sum("$amount") == {"$sum": "$amount"}

    def test_avg(self) -> None:
        assert e.avg("$rating") == {"$avg": "$rating"}

    def test_min(self) -> None:
        assert e.min("$price") == {"$min": "$price"}

    def test_max(self) -> None:
        assert e.max("$price") == {"$max": "$price"}

    def test_count_default(self) -> None:
        assert e.count() == {"$count": "*"}

    def test_count_with_field(self) -> None:
        assert e.count("$items") == {"$count": "$items"}


# ── Utility ─────────────────────────────────────────────────────────


class TestUtility:
    def test_field_reference(self) -> None:
        assert e.f("price") == "$price"

    def test_field_reference_nested(self) -> None:
        assert e.f("address.city") == "$address.city"


# ── Composition ─────────────────────────────────────────────────────


class TestComposition:
    def test_nested_arithmetic(self) -> None:
        result = e.multiply("$qty", e.subtract("$price", "$discount"))
        assert result == {
            "$multiply": ["$qty", {"$subtract": ["$price", "$discount"]}]
        }

    def test_complex_expression(self) -> None:
        result = e.cond(
            e.gt("$discount", 0),
            e.subtract("$total", e.multiply("$total", e.divide("$discount", 100))),
            "$total",
        )
        assert result == {
            "$cond": [
                {"$gt": ["$discount", 0]},
                {
                    "$subtract": [
                        "$total",
                        {"$multiply": ["$total", {"$divide": ["$discount", 100]}]},
                    ]
                },
                "$total",
            ]
        }

    def test_using_f_helper(self) -> None:
        result = e.multiply(e.f("qty"), e.f("price"))
        assert result == {"$multiply": ["$qty", "$price"]}
