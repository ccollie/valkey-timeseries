# TS.QUERYLABELS

Returns the distinct label names, or the distinct values of a single label, across the time
series matching a filter list.

```
TS.QUERYLABELS <LABELS | VALUES label> [FILTER selector [selector ...]]
```

`LABELS` returns the distinct label names carried by the matching series. `VALUES label`
returns the distinct values those series assign to `label`. The `FILTER` clause chooses which
series match; when it is omitted, every indexed series in the current database matches.

Each name or value appears at most once. The order of the reply is not part of the contract.

Unlike [`TS.QUERYINDEX`](ts.queryindex.md), which reports every matching key, `TS.QUERYLABELS`
silently omits series the caller may not read, so names and values belonging only to
unreadable series never appear in the result.

### Required arguments

<details open><summary><code>LABELS | VALUES label</code></summary>
The subtype, which must come first. `LABELS` selects distinct label names. `VALUES` selects
the distinct values of the single label name given as the next argument.
</details>

### Optional arguments

<details open><summary><code>selector</code></summary>
Repeated series selector that restricts which series are considered. See
[filter syntax](../topics/filter-syntax.md). At least one selector must follow `FILTER`.
</details>

#### Return

An [array](https://redis.io/docs/reference/protocol-spec#resp-arrays) of bulk strings under
RESP2, or a [set](https://redis.io/docs/latest/develop/reference/protocol-spec/#sets) under
RESP3. Empty when nothing matches.

#### Error

Returns an error reply in the following cases:

- The subtype is missing or is neither `LABELS` nor `VALUES` —
  `TSDB: unknown subtype, must be one of LABELS|VALUES`.
- `VALUES` is given without a label name — wrong number of arguments.
- A token other than `FILTER` follows the subtype —
  `TSDB: unknown argument, expected FILTER`.
- `FILTER` is given with no selectors —
  `TSDB: FILTER given with no filter expressions`.
- A selector is malformed — `TSDB: series selector is invalid`.
- Every selector is negative (for example only `label!=value`), so the list is unbounded —
  `TSDB: please provide at least one matcher`.

#### Examples

Given four series labelled by `city`, `vehicle_id`, and `metric`:

```
TS.CREATE fleet:nyc:101:speed LABELS city nyc vehicle_id 101 metric speed
TS.CREATE fleet:nyc:101:fuel  LABELS city nyc vehicle_id 101 metric fuel
TS.CREATE fleet:lax:202:speed LABELS city lax vehicle_id 202 metric speed
TS.CREATE fleet:lax:202:fuel  LABELS city lax vehicle_id 202 metric fuel
```

All label names across every indexed series:

```
127.0.0.1:6379> TS.QUERYLABELS LABELS
1) "city"
2) "metric"
3) "vehicle_id"
```

All values of `city`:

```
127.0.0.1:6379> TS.QUERYLABELS VALUES city
1) "lax"
2) "nyc"
```

All values of `metric`, restricted to the NYC fleet:

```
127.0.0.1:6379> TS.QUERYLABELS VALUES metric FILTER city=nyc
1) "fuel"
2) "speed"
```

#### See also

[`TS.QUERYINDEX`](ts.queryindex.md) | [`TS.LABELNAMES`](ts.labelnames.md) |
[`TS.LABELVALUES`](ts.labelvalues.md)
