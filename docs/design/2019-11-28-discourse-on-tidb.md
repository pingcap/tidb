# Proposal: Migrate Discourse's db from PostgreSQL to TiDB

- Author(s): EE-Team
- Last updated:  Nov 19, 2019
- Discussion at: https://github.com/pingcap-incubator/discourse/issues/

## Abstract

The proposal aim at:

1. **Eat our own dog food**;
2. Connect TiDB Community with Discourse Community and Ruby On Rails Community;

## Background

The TiDB User Group Website: [AskTUG](https://asktug.com) is running on Discourse, and Discourse is running on PostgreSQL, we want to migrate it to TiDB. Then we can frequently test TiDB before each release.

## Proposal

We will make these changes:

1. Make Discourse's most sql statemennt run on MySQL;
2. Add new component elastic search instead of PostgreSQL's fulltext search;
3. Migrate from MySQL to TiDB, fix potential compatible problems;

## Rationale

## Compatibility

## Implementation

## Open issues (if applicable)

