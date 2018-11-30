# Proposal: Support to restore SQL text from any `ast.Node`.

- Author(s):     [Yilin Zhao](https://github.com/leoppro)
- Last updated:  2018-11-29
- Discussion at: 

## Abstract

This proposal aims to support to restore SQL text from any `ast.Node`.

## Background

We know there is a `Text()` function of `ast.Node`, 
the parser calls `SetText()` function during the parsing process, then we can call `Text()` to get original input sql. 
But the `Text()` function is incomplete, only the `Text()` of root nodes can work well. 

Now some features(eg. Database name and table name mapping) need us to make any `ast.Node` could restore to SQL text.

## Proposal

<!--
A precise statement of the proposed change:
- The new named concepts and a set of metrics to be collected in this proposal (if applicable)
- The overview of the design.
- How it works?
- What needs to be changed to implement this design?
- What may be positively influenced by the proposed change?
- What may be negatively impacted by the proposed change?
-->

## Rationale

<!--
A discussion of alternate approaches and the trade-offs, advantages, and disadvantages of the specified approach:
- How other systems solve the same issue?
- What other designs have been considered and what are their disadvantages?
- What is the advantage of this design compared with other designs?
- What is the disadvantage of this design?
- What is the impact of not doing this?
-->

## Compatibility

<!--
A discussion of the change with regard to the compatibility issues:
- Does this proposal make TiDB not compatible with the old versions?
- Does this proposal make TiDB more compatible with MySQL?
-->

## Implementation

<!--
A detailed description for each step in the implementation:
- Does any former steps block this step?
- Who will do it?
- When to do it?
- How long it takes to accomplish it?
-->

## Open issues (if applicable)

<!--
A discussion of issues relating to this proposal for which the author does not know the solution. This section may be omitted if there are none.
-->
