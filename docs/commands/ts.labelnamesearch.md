## TS.LABELNAMESEARCH

Searches label names with substring and optional fuzzy matching.

```text
TS.LABELNAMESEARCH
  [SEARCH term [term ...]]
  [FUZZ_THRESHOLD threshold]
  [FUZZ_ALG jarowinkler|subsequence]
  [CASE_SENSITIVE true|false]
  [SORT_BY alpha|score]
  [SORT_DIR asc|dsc]
  [FILTER_BY_RANGE [NOT] fromTimestamp toTimestamp]
  [LIMIT limit]
  [FILTER selector ...]
```

### Notes

- `SEARCH` terms are ORed together.
- `FUZZ_THRESHOLD` accepts `0..100`.
- `SORT_BY score` currently supports `SORT_DIR dsc` only.
- In cluster mode this command fans out to all shards and merges results.

### Return

Array reply of matching label names.

### Example

```text
TS.LABELNAMESEARCH SEARCH node FUZZ_THRESHOLD 80 FUZZ_ALG jarowinkler SORT_BY score SORT_DIR dsc FILTER name=cpu
```

