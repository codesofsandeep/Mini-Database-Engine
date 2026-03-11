# minidb/query.py
import re
from typing import Any


# ══════════════════════════════════════════════
#  PART 1 — AST NODES (Condition Tree)
# ══════════════════════════════════════════════

class Condition:
    """Base class for all query conditions."""
    def evaluate(self, doc: dict) -> bool:
        raise NotImplementedError


class CompareCondition(Condition):
    """
    A single field comparison: age >= 18
    Supports: == != > >= < <= ~= (contains)
    """
    OPS = {
        "==": lambda a, b: a == b,
        "=" : lambda a, b: a == b,
        "!=": lambda a, b: a != b,
        ">" : lambda a, b: a >  b,
        ">=": lambda a, b: a >= b,
        "<" : lambda a, b: a <  b,
        "<=": lambda a, b: a <= b,
        "~=": lambda a, b: str(b).lower() in str(a).lower(),
    }

    def __init__(self, field: str, op: str, value: Any):
        self.field = field
        self.op    = op
        self.value = value

    def evaluate(self, doc: dict) -> bool:
        if not isinstance(doc, dict):
            return False
        # support dot notation: "address.city"
        val = doc
        for part in self.field.split("."):
            if isinstance(val, dict):
                val = val.get(part)
            else:
                return False
        if val is None:
            return False
        fn = self.OPS.get(self.op)
        if fn is None:
            return False
        try:
            return fn(type(self.value)(val), self.value)
        except (TypeError, ValueError):
            try:
                return fn(val, self.value)
            except TypeError:
                return False

    def __repr__(self):
        return f"{self.field} {self.op} {self.value!r}"


class AndCondition(Condition):
    def __init__(self, left: Condition, right: Condition):
        self.left  = left
        self.right = right

    def evaluate(self, doc: dict) -> bool:
        return self.left.evaluate(doc) and self.right.evaluate(doc)

    def __repr__(self):
        return f"({self.left} AND {self.right})"


class OrCondition(Condition):
    def __init__(self, left: Condition, right: Condition):
        self.left  = left
        self.right = right

    def evaluate(self, doc: dict) -> bool:
        return self.left.evaluate(doc) or self.right.evaluate(doc)

    def __repr__(self):
        return f"({self.left} OR {self.right})"


class NotCondition(Condition):
    def __init__(self, cond: Condition):
        self.cond = cond

    def evaluate(self, doc: dict) -> bool:
        return not self.cond.evaluate(doc)

    def __repr__(self):
        return f"NOT({self.cond})"


# ══════════════════════════════════════════════
#  PART 2 — TOKENIZER (Lexer)
# ══════════════════════════════════════════════

# Regex matches tokens in this priority order:
TOKEN_RE = re.compile(
    r'(\(|\)'                        # parentheses
    r'|\band\b|\bor\b|\bnot\b'       # keywords
    r'|==|!=|>=|<=|>|<|~='           # operators
    r'|"[^"]*"|\'[^\']*\''           # quoted strings
    r'|true|false|null'              # literals
    r'|[-+]?\d+(?:\.\d+)?'          # numbers
    r'|[\w.]+)',                     # identifiers / field names
    re.IGNORECASE
)

def tokenize(s: str) -> list:
    return [m.group() for m in TOKEN_RE.finditer(s)]

def parse_value(token: str) -> Any:
    """Convert a token string to its Python value."""
    if token.lower() == "true":  return True
    if token.lower() == "false": return False
    if token.lower() == "null":  return None
    if (token.startswith('"') and token.endswith('"')) or \
       (token.startswith("'") and token.endswith("'")):
        return token[1:-1]
    try:    return int(token)
    except ValueError: pass
    try:    return float(token)
    except ValueError: pass
    return token


# ══════════════════════════════════════════════
#  PART 3 — RECURSIVE DESCENT PARSER
# ══════════════════════════════════════════════
#
#  Grammar (lowest → highest precedence):
#    expr   → or_expr
#    or     → and (OR and)*
#    and    → not (AND not)*
#    not    → NOT atom | atom
#    atom   → ( expr ) | field op value

class QueryParser:
    """
    Parses a query string into a Condition tree (AST).

    Examples:
        "age >= 18"
        "city == 'NY' AND active == true"
        "score > 90 OR age < 20"
        "(city == 'NY' OR city == 'LA') AND active == true"
        "name ~= 'ali'"
    """

    def parse(self, query_str: str) -> Condition:
        self.tokens = tokenize(query_str)
        self.pos    = 0
        cond = self._parse_or()
        return cond

    def _peek(self):
        if self.pos < len(self.tokens):
            return self.tokens[self.pos]
        return None

    def _consume(self):
        token = self.tokens[self.pos]
        self.pos += 1
        return token

    def _parse_or(self) -> Condition:
        left = self._parse_and()
        while self._peek() and self._peek().lower() == "or":
            self._consume()
            right = self._parse_and()
            left  = OrCondition(left, right)
        return left

    def _parse_and(self) -> Condition:
        left = self._parse_not()
        while self._peek() and self._peek().lower() == "and":
            self._consume()
            right = self._parse_not()
            left  = AndCondition(left, right)
        return left

    def _parse_not(self) -> Condition:
        if self._peek() and self._peek().lower() == "not":
            self._consume()
            cond = self._parse_atom()
            return NotCondition(cond)
        return self._parse_atom()

    def _parse_atom(self) -> Condition:
        token = self._peek()

        # Parenthesised sub-expression
        if token == "(":
            self._consume()             # eat "("
            cond = self._parse_or()
            if self._peek() == ")":
                self._consume()         # eat ")"
            return cond

        # field op value
        if self.pos + 2 >= len(self.tokens):
            raise ValueError(
                f"Incomplete expression at token {self.pos}: {token!r}"
            )
        field = self._consume()
        op    = self._consume()
        raw   = self._consume()
        value = parse_value(raw)

        if op not in CompareCondition.OPS:
            raise ValueError(f"Unknown operator: {op!r}")

        return CompareCondition(field, op, value)


# ══════════════════════════════════════════════
#  PART 4 — QUERY RESULT
# ══════════════════════════════════════════════

class QueryResult:
    """
    Wraps query output with metadata.
    Tells you HOW the query ran, not just what it returned.
    """
    def __init__(self, docs: list, scanned: int, index_used: str):
        self.docs        = docs           # list of dicts
        self.scanned     = scanned        # docs examined
        self.total       = len(docs)      # docs matched
        self.index_used  = index_used     # field name or "none"

    def __repr__(self):
        return (f"QueryResult(matched={self.total}, "
                f"scanned={self.scanned}, "
                f"index={self.index_used!r})")


# ══════════════════════════════════════════════
#  PART 5 — EXECUTOR
# ══════════════════════════════════════════════

class QueryExecutor:
    """
    Runs a parsed Condition against a collection's data.

    Strategy:
        1. Detect if the root condition is a simple CompareCondition
           with an available index → use index to get candidate keys
        2. Otherwise → full scan all documents
        3. Apply condition.evaluate() on candidates
        4. Sort (ORDER BY) and slice (LIMIT)
    """

    def __init__(self, index_manager=None):
        self._parser  = QueryParser()
        self._indexes = index_manager

    def execute(self, query_str: str, all_docs: dict,
                limit: int = None,
                order_by: str = None,
                order_desc: bool = False) -> QueryResult:

        # Empty query → return everything
        if not query_str.strip():
            docs = [{"_key": k, **v} if isinstance(v, dict)
                    else {"_key": k, "_value": v}
                    for k, v in all_docs.items()]
            if order_by:
                docs.sort(key=lambda d: d.get(order_by, ""),
                          reverse=order_desc)
            if limit is not None:
                docs = docs[:limit]
            return QueryResult(docs, len(all_docs), "none")

        condition = self._parser.parse(query_str)

        # ── Try index acceleration ──────────────────
        candidate_keys = None
        index_used     = "none"

        if self._indexes and isinstance(condition, CompareCondition):
            field = condition.field
            if self._indexes.has(field):
                candidate_keys = self._indexes.query(
                    field, condition.op, condition.value
                )
                if candidate_keys is not None:
                    index_used = field

        # ── Build scan set ──────────────────────────
        if candidate_keys is not None:
            scan = {k: all_docs[k] for k in candidate_keys
                    if k in all_docs}
        else:
            scan = all_docs

        # ── Filter ──────────────────────────────────
        results = []
        for key, doc in scan.items():
            doc_view = {"_key": key}
            if isinstance(doc, dict):
                doc_view.update(doc)
            else:
                doc_view["_value"] = doc
            if condition.evaluate(doc_view):
                results.append(doc_view)

        scanned = len(scan)

        # ── Order by ────────────────────────────────
        if order_by:
            results.sort(
                key=lambda d: d.get(order_by, ""),
                reverse=order_desc
            )

        # ── Limit ───────────────────────────────────
        if limit is not None:
            results = results[:limit]

        return QueryResult(results, scanned, index_used)