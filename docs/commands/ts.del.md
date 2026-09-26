# TS.DEL

Delete samples from a time series within an inclusive timestamp range.

## Syntax

```
TS.DEL key fromTimestamp toTimestamp
```

## Arguments

- `key` — Existing time-series key.
- `fromTimestamp` — Start of the range, inclusive, in milliseconds since the Unix epoch.
- `toTimestamp` — End of the range, inclusive, in milliseconds since the Unix epoch.

The timestamp range may use the usual range forms such as `-` and `+`.
Setting `fromTimestamp` equal to `toTimestamp` deletes only the sample at
that timestamp, if one exists.

## Behavior

- Deletes every sample whose timestamp is between the two boundaries, including both boundaries.
- Returns `0` when the range contains no samples; this is not an error.
- Deletion can span multiple chunks.
- If the series participates in compaction rules, affected destination series are updated to reflect the deletion.
- The time-series key itself is retained, even when all its samples are deleted.
- Keyspace notifications use the `ts.del` event.

## Return value

Returns the number of samples deleted as an integer.

## Errors

- `ERR wrong number of arguments` — The key or either timestamp is missing.
- A key-does-not-exist error — The key does not exist.
- `WRONGTYPE` — The key exists but is not a time series.
- `TSDB: invalid timestamp` — A timestamp cannot be parsed.
- `TSDB: error deleting range` — An internal error occurred while deleting the range.

## Complexity

O(N), where N is the number of samples removed.

## ACL categories

`@write`, `@fast`, `@timeseries`

## Examples

Delete a range of samples, including both endpoints:

```
TS.DEL requests:status:200 587396550 1587396550
```

Delete one sample at timestamp `1609459200000`:

```
TS.DEL requests:status:200 1609459200000 1609459200000
```
