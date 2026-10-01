"""
Statement rewrites that work around result-changing limits of the SQL plugin.

Every helper reads the statement with string literals and comments blanked to
the same length (so offsets stay valid in the original text) and tracks
parenthesis depth, so words inside literals, comments or subqueries never
match a top-level clause.
"""

import re
from typing import Dict, List, NamedTuple, Optional, Tuple

_LITERAL_OR_COMMENT_RE = re.compile(r"'(?:[^']|'')*'|--[^\n]*|/\*.*?\*/", re.S)
_IDENT = r'(?:"[^"]+"|`[^`]+`|[A-Za-z_][\w@]*)'
_ALIAS_RE = re.compile(rf"\bAS\s+({_IDENT})", re.IGNORECASE)
_QUALIFIED_RE = re.compile(rf"{_IDENT}\s*\.\s*({_IDENT})")
_SUBQUERY_RE = re.compile(r"\(\s*SELECT\b", re.IGNORECASE)
_CALL_RE = re.compile(r"\s*\(")
_FIELD_RE = re.compile(r"[A-Za-z_][\w@]*")
_LIMIT_RE = re.compile(r"\bLIMIT\s+(\d+)(\s+OFFSET\s+\d+)?\s*;?\s*$", re.IGNORECASE)


def blank_literals(query: str) -> str:
    """``query`` with string literals and comments replaced by spaces."""
    return _LITERAL_OR_COMMENT_RE.sub(lambda m: " " * len(m.group(0)), query)


def _depths(text: str) -> List[int]:
    depths, depth = [], 0
    for char in text:
        if char == "(":
            depth += 1
        depths.append(depth)
        if char == ")":
            depth -= 1
    return depths


def _top_level(text: str, pattern: str, start: int = 0) -> Optional[Tuple[int, int]]:
    """Span of the first match of ``pattern`` at depth 0, from ``start``."""
    depths = _depths(text)
    compiled = re.compile(pattern, re.IGNORECASE | re.MULTILINE)
    for match in compiled.finditer(text, start):
        if depths[match.start()] == 0:
            return match.start(), match.end()
    return None


def _unquote(identifier: str) -> str:
    if identifier[:1] in ('"', "`"):
        return identifier[1:-1]
    return identifier


def has_subquery(query: str) -> bool:
    """Whether ``query`` contains a parenthesised SELECT."""
    return bool(_SUBQUERY_RE.search(blank_literals(query)))


def rename_colliding_aliases(query: str) -> Tuple[str, Dict[str, str]]:
    """
    Renames select-list aliases that a table-qualified column elsewhere in the
    statement shares a name with, e.g. ``v AS k`` next to ``ORDER BY grp.k``.

    The SQL plugin's legacy engine resolves ``grp.k`` to the alias ``k`` (so
    it sorts by ``v``), unlike SQL and the v2 engine. Bare references to the
    alias in GROUP BY, HAVING and ORDER BY are renamed too. Returns the
    statement and a map from each new alias to the original one, to restore
    the column names of the result.
    """
    text = blank_literals(query)
    select = _top_level(text, r"\A\s*SELECT\b")
    from_ = _top_level(text, r"\bFROM\b")
    if not select or not from_:
        return query, {}
    list_start, list_end = select[1], from_[0]
    select_list = text[list_start:list_end]
    rest = text[list_end:]
    qualified = {_unquote(m.group(1)).lower() for m in _QUALIFIED_RE.finditer(rest)}
    if not qualified:
        return query, {}
    depths = _depths(select_list)
    edits: List[Tuple[int, int, str]] = []
    renames: Dict[str, str] = {}
    item_start = 0
    for match in _ALIAS_RE.finditer(select_list):
        if depths[match.start()] != 0:
            continue
        commas = [
            i
            for i in range(item_start, match.start())
            if select_list[i] == "," and depths[i] == 0
        ]
        expression_start = commas[-1] + 1 if commas else item_start
        expression_end = match.start()
        expression = select_list[expression_start:expression_end].strip()
        item_start = match.end()
        alias = _unquote(match.group(1))
        if alias.lower() not in qualified:
            continue
        own_column = (
            re.fullmatch(rf"(?:{_IDENT}\s*\.\s*)?{_IDENT}", expression)
            and _unquote(re.split(r"\s*\.\s*", expression)[-1]).lower() == alias.lower()
        )
        if own_column:
            continue
        new = f"{alias}__es{len(renames)}"
        renames[new] = alias
        offset = list_start + match.start(1)
        edits.append((offset, offset + len(match.group(1)), new))
    if not renames:
        return query, {}
    alias_clauses = _top_level(text, r"\b(?:GROUP\s+BY|HAVING|ORDER\s+BY)\b", list_end)
    if alias_clauses:
        clause_start = alias_clauses[1]
        limit = _top_level(text, r"\bLIMIT\b", clause_start)
        clause_end = limit[0] if limit else len(text)
        clause = text[clause_start:clause_end]
        by_name = {old.lower(): new for new, old in renames.items()}
        for ref in re.finditer(_IDENT, clause):
            ref_start, ref_end = ref.span()
            before = clause[:ref_start].rstrip()
            after = clause[ref_end:].lstrip()
            if before.endswith(".") or after.startswith((".", "(")):
                continue
            renamed = by_name.get(_unquote(ref.group(0)).lower())
            if renamed:
                offset = clause_start + ref_start
                edits.append((offset, offset + len(ref.group(0)), renamed))
    for edit_start, edit_end, new in sorted(edits, reverse=True):
        query = query[:edit_start] + new + query[edit_end:]
    return query, renames


def limit_subqueries(query: str, window: int) -> Tuple[str, List[str]]:
    """
    Gives every parenthesised SELECT without a LIMIT of its own ``LIMIT
    window``: the SQL plugin runs such a subquery like a top-level query and
    stops it at its size limit (200 rows by default on Open Distro), silently
    leaving rows out of the outer result. Returns the statement and the
    subqueries that were limited (their own text, without the added LIMIT).
    """
    limited: List[str] = []
    position = len(query)
    while True:
        text = blank_literals(query)
        starts = [m for m in _SUBQUERY_RE.finditer(text) if m.start() < position]
        if not starts:
            return query, limited
        open_paren = starts[-1].start()
        position = open_paren
        depths = _depths(text)
        close = next(
            (
                i
                for i in range(open_paren + 1, len(text))
                if text[i] == ")" and depths[i] == depths[open_paren]
            ),
            None,
        )
        if close is None:
            return query, limited
        inner_start = open_paren + 1
        inner = query[inner_start:close]
        if _LIMIT_RE.search(blank_literals(inner).rstrip()):
            continue
        limited.append(inner.rstrip())
        inner = f"{inner.rstrip()}\nLIMIT {window}\n"
        query = query[:inner_start] + inner + query[close:]


class OuterClauses(NamedTuple):
    where: bool
    order_by: bool
    limit: Optional[int]
    limit_start: Optional[int]
    offset: bool


def outer_clauses(query: str) -> OuterClauses:
    """The top-level WHERE, ORDER BY and trailing LIMIT of ``query``."""
    text = blank_literals(query)
    limit = _LIMIT_RE.search(text)
    if limit and _depths(text)[limit.start()] != 0:
        limit = None
    return OuterClauses(
        where=bool(_top_level(text, r"\bWHERE\b")),
        order_by=bool(_top_level(text, r"\bORDER\s+BY\b")),
        limit=int(limit.group(1)) if limit else None,
        limit_start=limit.start() if limit else None,
        offset=bool(limit and limit.group(2)),
    )


def _split_top_level(text: str, start: int, end: int) -> List[Tuple[int, int]]:
    """Spans of the items of ``text[start:end]`` between its depth-0 commas."""
    depths = _depths(text)
    spans = []
    for i in range(start, end):
        if text[i] == "," and depths[i] == 0:
            spans.append((start, i))
            start = i + 1
    spans.append((start, end))
    return spans


def _normalize(expression: str) -> str:
    """
    ``expression`` as single-spaced tokens, without identifier quotes or
    table qualifiers, with function names lowercased; literals and field
    names are kept as is.
    """
    expression = re.sub(
        rf"('(?:[^']|'')*')|{_QUALIFIED_RE.pattern}",
        lambda m: m.group(1) or m.group(2),
        expression,
    )
    parts = []
    for token in re.finditer(rf"'(?:[^']|'')*'|{_IDENT}|\S", expression):
        part = _unquote(token.group(0))
        if _CALL_RE.match(expression, token.end()):
            part = part.lower()
        parts.append(part)
    return " ".join(parts)


def _orders_by_group_keys(
    query: str, text: str, group: Tuple[int, int], order: Tuple[int, int]
) -> bool:
    """
    Whether every ORDER BY item is a field the statement groups by, by its
    name, its select-list alias or its ordinal. The v2 engine sorts such a
    statement inside the composite aggregation, so its answer is complete.
    Anything less certain is ``False``: HAVING, joins, subqueries, and
    expression keys (whose script values the server sorts as strings).
    """
    if has_subquery(query) or _top_level(text, r"\b(?:HAVING|JOIN)\b"):
        return False
    select = _top_level(text, r"\A\s*SELECT\b")
    from_ = _top_level(text, r"\bFROM\b")
    limit_start = outer_clauses(query).limit_start
    if not select or not from_ or limit_start is None or group[1] > order[0]:
        return False
    # comments blanked, literals kept: they distinguish expressions
    source = _LITERAL_OR_COMMENT_RE.sub(
        lambda m: m.group(0) if m.group(0)[0] == "'" else " " * len(m.group(0)),
        query,
    )
    selected: List[str] = []
    aliases: Dict[str, str] = {}
    for start, end in _split_top_level(text, select[1], from_[0]):
        alias = re.search(rf"\s+AS\s+({_IDENT})\s*$", text[start:end], re.I)
        expression_end = start + alias.start() if alias else end
        selected.append(_normalize(source[start:expression_end]))
        if alias:
            aliases[_unquote(alias.group(1))] = selected[-1]

    def field(expression: str) -> Optional[str]:
        expression = aliases.get(expression, expression)
        return expression if _FIELD_RE.fullmatch(expression) else None

    keys = {
        field(_normalize(source[start:end]))
        for start, end in _split_top_level(text, group[1], order[0])
    }
    keys.discard(None)
    for start, end in _split_top_level(text, order[1], limit_start):
        direction = re.search(
            r"(?:\s+(?:ASC|DESC))?(?:\s+NULLS\s+(?:FIRST|LAST))?\s*$",
            text[start:end],
            re.I,
        )
        if direction:
            end = start + direction.start()
        expression = _normalize(source[start:end])
        if expression.isdigit():
            ordinal = int(expression)
            if not 0 < ordinal <= len(selected):
                return False
            expression = selected[ordinal - 1]
        if field(expression) not in keys:
            return False
    return True


def grouped_order_probe(query: str) -> Optional[str]:
    """
    Removes HAVING/ORDER BY/LIMIT from a grouped top-N statement. The v2
    engine applies those operators after an incomplete composite aggregation,
    so a small final result cannot establish that it considered every group.
    The unfiltered group listing exposes the underlying bucket ceiling.
    Statements ordered only by their group keys need no probe.
    """
    text = blank_literals(query)
    group = _top_level(text, r"\bGROUP\s+BY\b")
    order = _top_level(text, r"\bORDER\s+BY\b")
    if not group or not order or outer_clauses(query).limit is None:
        return None
    if _orders_by_group_keys(query, text, group, order):
        return None
    having = _top_level(text, r"\bHAVING\b", group[1])
    end = having[0] if having else order[0]
    return query[:end].rstrip()
