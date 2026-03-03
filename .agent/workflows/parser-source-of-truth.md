---
description: parser.y is the source of truth for the TiDB SQL parser grammar
---

# Parser Source of Truth

`pkg/parser/parser.y` is the **source of truth** for the TiDB SQL parser grammar.

## Key Rules

- When investigating parser behavior, always read `parser.y` directly to verify grammar rules and precedence declarations.
- The hparser (`pkg/parser/hparser/`) must produce ASTs that are **semantically equivalent** to what the goyacc grammar in `parser.y` defines.
- When there's a discrepancy between hparser behavior and test expectations, verify against `parser.y` first before changing either.

## Important Grammar Sections

- **Precedence declarations**: Lines ~1655–1728, ordered LOW to HIGH (top to bottom)
- **TableRefs / JoinTable rules**: Lines ~10182–10389
- **NewCrossJoin**: Called only for cross joins WITHOUT ON (`%prec tableRefPriority` rule)
- **JoinType**: Only `LEFT` and `RIGHT` (line 10391–10399)
- **CrossOpt**: `JOIN | CROSS JOIN | INNER JOIN` (line 10405–10408)

## LALR Join Parsing Behavior

Key precedence for join parsing:
```
%left tableRefPriority           // line 1696 — LOW (used by cross join without ON)
%left join inner cross left ...  // line 1701 — HIGHER (join keywords)
%precedence lowerThanOn          // line 1702
%precedence on using             // line 1703 — HIGHEST (ON/USING tokens)
```

At state `TableRef CrossOpt TableRef .`:
- Lookahead `ON` → SHIFT wins over R1 reduce (ON > tableRefPriority)
- Lookahead `JOIN` → SHIFT wins (join > tableRefPriority) because `TableRef` at this position can also be `$1` of a new inner JoinTable
- This produces **right-leaning** trees for stacked joins with deferred ON clauses
