# Package support evidence

Each schema-2 package owner with support inventory writes exactly one
`<owner>-support.tsv` file here. Rows have five tab-separated fields:

```text
support_path  sha256  disposition  evidence_artifact  note
```

The accepted dispositions are `runtime-transcreated`, `test-transcreated`,
`build-metadata-reviewed`, and `generated-input-reviewed`. The path and SHA-256
must exactly match the checked package-support inventory, the evidence artifact
must exist, and the note must record the reviewed outcome. Campaign close
rejects missing, duplicate, foreign, or stale rows.
