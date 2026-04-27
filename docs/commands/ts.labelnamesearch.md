## TS.LABELNAMESEARCH

Searches label names with substring and optional fuzzy matching.

```text
TS.LABELNAMESEARCH
  [FILTER_BY_RANGE [NOT] fromTimestamp toTimestamp]
  [SEARCH term [term ...]]
  [FUZZ_THRESHOLD threshold]
  [FUZZ_ALG jarowinkler|subsequence]
  [IGNORE_CASE true|false]
  [SORT_BY alpha|score]
  [SORT_DIR asc|dsc]
  [LIMIT limit]
  [FILTER selector ...]
```

### Optional Arguments

- `SEARCH` terms are ORed together.
- `FUZZ_ALG` the fuzzy matching algorithm. Accepts `jarowinkler` and `subsequence`. Defaults to `jarowinkler`.
- `FUZZ_THRESHOLD` accepts `0..100`.
- `IGNORE_CASE` toggle case sensitivity in string matching. Defaults to `false`.
- `SORT_BY` detrmine how matching metrics should be sorted in the response. `SORT_DIR dsc` only.
- `SORT_DIR` determine the sort direction of the response. `SORT_BY score` only supports `SORT_DIR dsc`.
- `FILTER_BY_RANGE` limits the result to only labels from series which have data in the date range [`fromTimestamp` ..
  `toTimestamp`]. If `NOT` is specified, this filter is inverted to exclude labels from series with data in the
  specified date range.
- `LIMIT` limits the number of results returned.
- `FILTER` is a repeated series selector argument that selects the series to search for matching label names. At least
  one selector argument must be provided.
- In cluster mode this command fans out to all shards and merges results.

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

