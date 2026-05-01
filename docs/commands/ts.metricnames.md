## TS.METRICNAMES

Searches metric names (`__name__` label values) with substring and optional fuzzy matching.

```text
TS.METRICNAMES
  [SEARCH term [term ...]]
  [FUZZY_THRESHOLD threshold]
  [FUZZY_ALGORITHM jarowinkler|subsequence]
  [IGNORE_CASE true|false]
  [INCLUDE_METADATA true|false]
  [SORTBY <value|score|cardinality> [ASC|DESC]
  [FILTER_BY_RANGE [NOT] fromTimestamp toTimestamp]
  [LIMIT limit]
  [FILTER selector ...]
```

### Optional Arguments

- `SEARCH` terms are ORed together.
- `FUZZY_ALGORITHM` accepts `jarowinkler` and `subsequence`. Defaults to `jarowinkler`.
- `FUZZY_THRESHOLD` accepts `0..100`.
- `IGNORE_CASE` toggles case sensitivity in string matching. Defaults to `false`.
- `INCLUDE_METADATA` set to true to return `score` and `cardinality` in addition to metric names.
- `SORTBY` specify the sort order of the results.
- `FILTER_BY_RANGE` limits results to series with data in the given range. With `NOT`, this filter is inverted.
- `LIMIT` bounds the number of results returned.
- `FILTER` applies one or more series selectors.
- In cluster mode this command fans out to all shards and merges results.

### Return

Array reply of matching metric names.

### Example

```text
TS.METRICNAMES SEARCH cpu FUZZY_THRESHOLD 0.80 FUZZY_ALGORITHM jarowinkler SORTBY score DESC FILTER env=prod
```

