# Proposal: Support SQL Plan Management

- Author(s): [Haibin Xie](https://github.com/lamxTyler)
- Last updated: 2018-12-11
- Discussion at:

## Abstract

This proposal aims to support the SQL plan management. With the help of it, we can force the optimizer to choose a certain plan without modifying the SQL text.

## Background

The optimizer chooses a plan based on several environmental factors, such as statistics, optimizer parameters, schema definitions and so on. Once the environment changes, we cannot guarantee that the newly optimized plan is always better than the old plan. Therefore we need to provide ways to bind the plan for applications that cannot take the risk of a changed plan.

## Proposal

The following proposal mainly focuses on two parts: how to bind the plan and what is the syntax to manage it.

### How to bind the plan

In order to bind the plan, we need to maintain a mapping from normalized SQL text to plan. To normalize the SQL text, we can remove all the blank space, replace the parameters with placement markers, and convert remaining parts to lower cases. The most difficult problem is how we represent and store the plan.

One way to represent the plan is using the optimized physical plan. However, it is difficult to perform the parameters replacement for later SQLs, because some parameters may already be rewritten in the optimized physical plan when doing logical and physical optimizations.

Since the parameters replacement is hard, it is better not doing it. Another way to represent the plan is using the AST of hinted SQL, so the only thing needs to do for later SQLs is to traverse the AST and copy hints.

### Syntax to manage the binding plan

To manage the SQL bindings, we need to support basic operations like create, show and drop. We can also support SQL bindings that only exist in the current session. The syntax will be like the following:

- CREATE [GLOBAL|SESSION] BINDING_NAME BINDING FOR `SQL` USING `HINTED SQL`
- DROP [GLOBAL|SESSION] BINDINGS
- DROP [GLOBAL|SESSION] BINDING BINDING_NAME
- SHOW [GLOBAL|SESSION] BINDINGS [SHOW_LIKE_OR_WHERE]

## Rationale

In Oracle, they only store the hints of the optimized query, instead of the whole AST. For TiDB, it requires more work to do it now because we need to generate unique identifiers for the subqueries and lift all hints to the outer most queries. Storing the AST is the simplest way now.

## Compatibility

MySQL does not support SQL plan management, so this will add syntaxes that not supported by MySQL.

## Implementation

To implement it, we need the following main steps:

- Normalize the SQL text. We can take this https://github.com/pingcap/parser/pull/32 as an example.
- Support the syntax in the parser.
- Store the binding SQL and AST of the hinted SQL in a system table. Since there is a unique mapping from SQL text to AST, we can just store the SQL and parse it to AST for later use. A background goroutine will check if there are new bindings and update the local cache.
- When another SQL comes, we first check if there is a matched SQL in the cache. If so, we can traverse the AST to add hints. Since comparing text for every SQL may affect unrelated SQLs a lot, we can calculate a hash value and first check if there is matching hash values.

## Open issues (if applicable)
