<!--
This is a template for TiDB's change proposal process, documented [here](./README.md).
-->

# Proposal: <!-- Title -->

- Author(s):     <!-- Author Name, Co-Author Name, with the link(s) of the GitHub profile page -->
- Last updated:  <!-- Date -->
- Discussion at: <!-- https://github.com/pingcap/tidb/issues/XXX -->

## Table of Contents

* [Introduction](#introduction)
* [Motivation or Background](#motivation-or-background)
* [Detailed Design](#detailed-design)
* [Test Design](#test-design)
    * [Functional Tests](#functional-tests)
    * [Scenario Tests](#scenario-tests)
    * [Compatibility Tests](#compatibility-tests)
    * [Benchmark Tests](#benchmark-tests)
* [Impacts & Risks](#impacts--risks)
* [Investigation & Alternatives](#investigation--alternatives)
* [Unresolved Questions](#unresolved-questions)

## Introduction

<!--
One para explanation of the proposal. It’s recommended to write this section in English to help others get the brief info of this design doc.
-->

## Motivation or Background

<!--
What’s the background and the problem being solved by this design doc? What use cases does it support? What is the expected outcome?
-->

## Detailed Design

<!--
Explain the design in enough detail that: it is reasonably clear how the feature would be implemented, corner cases are dissected by example, how the feature is used, etc.

It’s better to describe the pseudo-code of the key algorithm, API interfaces, the UML graph, what components are needed to be changed in this section.

Compatibility is important, please also take into consideration, a checklist:
- Compatibility with other features, like partition table, security&privilege, collation&charset, clustered index, async commit, etc.
- Compatibility with other internal components, like parser, DDL, planner, statistics, executor, etc.
- Compatibility with other external components, like PD, TiKV, TiFlash, BR, TiCDC, Dumpling, TiUP, K8s, etc.
- Upgrade compatibility
- Downgrade compatibility
-->

## Test Design

<!--
A brief description of how the implementation will be tested. Both the integration test and the unit test should be considered.
-->

### Functional Tests

<!--
It’s used to ensure the basic feature function works as expected. Both the integration test and the unit test should be considered.
-->

### Scenario Tests

<!--
It’s used to ensure this feature works as expected in some common scenarios
-->

### Compatibility Tests

<!--
A checklist to test compatibility:
- Compatibility with other features, like partition table, security & privilege, charset & collation, clustered index, async commit, etc.
- Compatibility with other internal components, like parser, DDL, planner, statistics, executor, etc.
- Compatibility with other external components, like PD, TiKV, TiFlash, BR, TiCDC, Dumpling, TiUP, K8s, etc.
- Upgrade compatibility
- Downgrade compatibility
-->

### Benchmark Tests

<!--
The following two parts need to be measured:
- The performance of this feature under different parameters
- The performance influence on the online workload
-->

## Impacts & Risks

<!--
Describe the potential impacts & risks of the design on overall performance, security, k8s, and other aspects. List all the risks or unknowns by far.

Please describe impacts and risks in two sections: Impacts could be positive or negative, and intentional. Risks are usually negative, unintentional, and may or may not happen. E.g., for performance, we might expect a new feature to improve latency by 10% (expected impact), there is a risk that latency in scenarios X and Y could degrade by 50%.
-->

## Investigation & Alternatives

<!--
How do other systems solve this issue? What other designs have been considered and what is the rationale for not choosing them?
-->

## Unresolved Questions

<!--
What parts of the design are still to be determined?
-->
