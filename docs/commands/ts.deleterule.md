# TS.DELETERULE

Delete the compaction rule between a source and destination time series.

## Syntax

```
TS.DELETERULE sourceKey destKey
```

## Arguments

- `sourceKey` — The source time series that has the compaction rule.
- `destKey` — The destination time series targeted by the rule.

The source and destination keys must be different. Both keys must already
exist as time series, and a rule must exist for this exact source and
destination pair.

## Behavior

- Removes the rule from `sourceKey`.
- Stops future compaction from `sourceKey` into `destKey`.
- Clears the source-series metadata from `destKey`.
- Does not delete `destKey` or any samples already stored in it.
- When the source has multiple rules, removes only the rule targeting the specified destination.

## Return value

Returns `OK` on success.

## Errors

- `ERR wrong number of arguments` — Either key argument is missing, or an extra argument was provided.
- `TSDB: compaction rule does not exist` — The source and destination are the same, the destination does not exist, or no rule exists for the specified pair.
- A missing source key returns a key-does-not-exist error.
- `WRONGTYPE` — Either key exists but is not a time series.
- An ACL error is returned when the caller lacks `UPDATE` permission on either key.

## Complexity

O(1).

## ACL categories

`@write`, `@fast`, `@timeseries`

## Example

Create a source and destination series and connect them with a compaction
rule:

```
TS.CREATE readings:raw
TS.CREATE readings:hourly
TS.CREATERULE readings:raw readings:hourly AGGREGATION avg 1h
```

Delete the rule while preserving the destination series and its existing
data:

```
TS.DELETERULE readings:raw readings:hourly
```

The destination can be used again in a new rule after deletion:

```
TS.CREATERULE readings:raw readings:hourly AGGREGATION sum 1h
```
