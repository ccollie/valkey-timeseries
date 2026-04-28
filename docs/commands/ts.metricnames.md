## TS.METRICNAMES

Searches metric names (`__name__` label values) with substring and optional fuzzy matching.

```text
TS.METRICNAMES
  [SEARCH term [term ...]]
  [FUZZ_THRESHOLD threshold]
  [FUZZ_ALG jarowinkler|subsequence]
  [IGNORE_CASE true|false]
  [INCLUDE_SCORE true|false]
  [SORT_BY alpha|score]
  [SORT_DIR asc|dsc]
  [FILTER_BY_RANGE [NOT] fromTimestamp toTimestamp]
  [LIMIT limit]
  [FILTER selector ...]
```

### Optional Arguments

- `SEARCH` terms are ORed together.
- `FUZZ_ALG` accepts `jarowinkler` and `subsequence`. Defaults to `jarowinkler`.
- `FUZZ_THRESHOLD` accepts `0..100`.
- `IGNORE_CASE` toggles case sensitivity in string matching. Defaults to `false`.
- `INCLUDE_SCORE` returns `[metric_name, score]` pairs.
- `SORT_BY score` supports `SORT_DIR dsc` only.
- `FILTER_BY_RANGE` limits results to series with data in the given range. With `NOT`, this filter is inverted.
- `LIMIT` bounds the number of results returned.
- `FILTER` applies one or more series selectors.
- In cluster mode this command fans out to all shards and merges results.

### Return

Array reply of matching metric names.

### Example

```text
TS.METRICNAMES SEARCH cpu FUZZ_THRESHOLD 80 FUZZ_ALG jarowinkler SORT_BY score SORT_DIR dsc FILTER env=prod
```

