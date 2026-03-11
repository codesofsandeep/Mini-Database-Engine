import sys, os, pytest
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from minidb.query import (
    tokenize, parse_value, QueryParser,
    CompareCondition, AndCondition, OrCondition, NotCondition,
    QueryExecutor, QueryResult
)
from minidb.collection import Collection


# ── sample data used across tests ──────────────
USERS = {
    "u:1": {"name": "Alice",  "age": 28, "city": "New York",  "active": True,  "score": 92},
    "u:2": {"name": "Bob",    "age": 34, "city": "London",    "active": True,  "score": 75},
    "u:3": {"name": "Carol",  "age": 22, "city": "New York",  "active": False, "score": 88},
    "u:4": {"name": "David",  "age": 45, "city": "Tokyo",     "active": True,  "score": 61},
    "u:5": {"name": "Eve",    "age": 19, "city": "London",    "active": True,  "score": 97},
    "u:6": {"name": "Frank",  "age": 31, "city": "New York",  "active": True,  "score": 83},
}


class TestTokenizer:

    def test_simple_comparison(self):
        assert tokenize("age >= 18") == ["age", ">=", "18"]

    def test_quoted_string(self):
        tokens = tokenize('city == "New York"')
        assert tokens == ["city", "==", '"New York"']

    def test_and_or(self):
        tokens = tokenize("age > 18 AND city == 'NY'")
        assert "AND" in [t.upper() for t in tokens]

    def test_parentheses(self):
        tokens = tokenize("(age > 18)")
        assert tokens[0] == "("
        assert tokens[-1] == ")"


class TestParseValue:

    def test_integer(self):     assert parse_value("42")     == 42
    def test_float(self):       assert parse_value("3.14")   == 3.14
    def test_true(self):        assert parse_value("true")   is True
    def test_false(self):       assert parse_value("false")  is False
    def test_null(self):        assert parse_value("null")   is None
    def test_quoted_string(self):
        assert parse_value('"hello"') == "hello"
        assert parse_value("'world'") == "world"


class TestConditionEvaluate:

    def test_compare_eq(self):
        c = CompareCondition("city", "==", "NY")
        assert c.evaluate({"city": "NY"})   is True
        assert c.evaluate({"city": "LA"})   is False

    def test_compare_gte(self):
        c = CompareCondition("age", ">=", 18)
        assert c.evaluate({"age": 18})  is True
        assert c.evaluate({"age": 17})  is False

    def test_contains(self):
        c = CompareCondition("name", "~=", "ali")
        assert c.evaluate({"name": "Alice"})  is True
        assert c.evaluate({"name": "Bob"})    is False

    def test_and(self):
        c = AndCondition(
            CompareCondition("age",  ">=", 18),
            CompareCondition("city", "==", "NY")
        )
        assert c.evaluate({"age": 25, "city": "NY"}) is True
        assert c.evaluate({"age": 25, "city": "LA"}) is False

    def test_or(self):
        c = OrCondition(
            CompareCondition("age", "<",  18),
            CompareCondition("age", ">",  60)
        )
        assert c.evaluate({"age": 15})  is True
        assert c.evaluate({"age": 65})  is True
        assert c.evaluate({"age": 30})  is False

    def test_not(self):
        c = NotCondition(CompareCondition("active", "==", True))
        assert c.evaluate({"active": False}) is True
        assert c.evaluate({"active": True})  is False


class TestQueryParser:

    def test_simple(self):
        cond = QueryParser().parse("age >= 18")
        assert isinstance(cond, CompareCondition)
        assert cond.field == "age"
        assert cond.op    == ">="
        assert cond.value == 18

    def test_and_expression(self):
        cond = QueryParser().parse('age >= 18 AND city == "NY"')
        assert isinstance(cond, AndCondition)

    def test_or_expression(self):
        cond = QueryParser().parse("age < 18 OR age > 60")
        assert isinstance(cond, OrCondition)

    def test_not_expression(self):
        cond = QueryParser().parse("NOT active == true")
        assert isinstance(cond, NotCondition)

    def test_parentheses(self):
        cond = QueryParser().parse('(city == "NY" OR city == "LA") AND active == true')
        assert isinstance(cond, AndCondition)
        assert isinstance(cond.left, OrCondition)

    def test_invalid_operator_raises(self):
        with pytest.raises((ValueError, IndexError)):
            QueryParser().parse("age ?? 18")


class TestQueryExecutor:

    def setup_method(self):
        self.ex = QueryExecutor()

    def test_empty_query_returns_all(self):
        result = self.ex.execute("", USERS)
        assert result.total == 6

    def test_simple_filter(self):
        result = self.ex.execute("age >= 30", USERS)
        names = [d["name"] for d in result.docs]
        assert "Bob"   in names
        assert "David" in names
        assert "Alice" not in names

    def test_and_filter(self):
        result = self.ex.execute('city == "New York" AND active == true', USERS)
        names = [d["name"] for d in result.docs]
        assert "Alice" in names
        assert "Frank" in names
        assert "Carol" not in names    # inactive

    def test_or_filter(self):
        result = self.ex.execute("score > 90 OR age < 20", USERS)
        names = [d["name"] for d in result.docs]
        assert "Alice" in names   # score 92
        assert "Eve"   in names   # score 97 AND age 19

    def test_contains(self):
        result = self.ex.execute('name ~= "ali"', USERS)
        assert result.total == 1
        assert result.docs[0]["name"] == "Alice"

    def test_order_by_asc(self):
        result = self.ex.execute("age > 0", USERS, order_by="age")
        ages = [d["age"] for d in result.docs]
        assert ages == sorted(ages)

    def test_order_by_desc(self):
        result = self.ex.execute("age > 0", USERS,
                                 order_by="age", order_desc=True)
        ages = [d["age"] for d in result.docs]
        assert ages == sorted(ages, reverse=True)

    def test_limit(self):
        result = self.ex.execute("age > 0", USERS,
                                 order_by="score", order_desc=True, limit=3)
        assert result.total == 3

    def test_index_used(self):
        from minidb.index import IndexManager
        mgr = IndexManager("users")
        mgr.create("city")
        mgr.rebuild(USERS)
        ex = QueryExecutor(mgr)
        result = ex.execute('city == "Tokyo"', USERS)
        assert result.index_used == "city"
        assert result.scanned < 6          # didn't scan all 6

    def test_full_scan_when_no_index(self):
        result = self.ex.execute('city == "Tokyo"', USERS)
        assert result.index_used == "none"
        assert result.scanned == 6         # scanned all


class TestCollectionQuery:

    def setup_method(self):
        self.col = Collection("users")
        for key, doc in USERS.items():
            self.col.set(key, doc)

    def test_basic_query(self):
        result = self.col.query('age >= 30')
        assert result.total == 3

    def test_query_with_index(self):
        self.col.create_index("city")
        result = self.col.query('city == "New York"')
        assert result.index_used == "city"
        assert result.total == 3

    def test_query_order_and_limit(self):
        result = self.col.query("score > 0",
                                order_by="score",
                                order_desc=True,
                                limit=2)
        assert result.docs[0]["score"] > result.docs[1]["score"]
        assert result.total == 2

    def test_find_one(self):
        doc = self.col.find_one('name == "Alice"')
        assert doc is not None
        assert doc["name"] == "Alice"

    def test_find_one_missing(self):
        doc = self.col.find_one('name == "Nobody"')
        assert doc is None