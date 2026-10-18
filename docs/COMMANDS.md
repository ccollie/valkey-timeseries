# Command List

Standalone `TS.LABELNAMES`, `TS.LABELVALUES` and `TS.METRICNAMES` searches run on bounded
background workers when estimated work is large. Small searches, transactions and scripts run inline.

Ranked label-value and metric-name searches apply `LIMIT` after ranking; equal scores or
cardinalities are ordered by value ascending.

- [`TS.ADD`](commands/ts.add.md)
- [`TS.ADDBULK`](commands/ts.addbulk.md)
- [`TS.ALTER`](commands/ts.alter.md)
- [`TS.CARD`](commands/ts.card.md) — index cardinality or a range-filtered count, read in batches per shard in cluster mode.
- [`TS.CREATE`](commands/ts.create.md)
- [`TS.CREATERULE`](commands/ts.createrule.md)
- [`TS.DECRBY`](commands/ts.decrby.md)
- [`TS.DEL`](commands/ts.del.md)
- [`TS.DELETERULE`](commands/ts.deleterule.md)
- [`TS.GET`](commands/ts.get.md)
- [`TS.INCRBY`](commands/ts.incrby.md)
- [`TS.INFO`](commands/ts.info.md)
- [`TS.JOIN`](commands/ts.join.md)
- [`TS.LABELNAMES`](commands/ts.labelnames.md)
- [`TS.LABELSTATS`](commands/ts.labelstats.md)
- [`TS.LABELVALUES`](commands/ts.labelvalues.md)
- [`TS.MADD`](commands/ts.madd.md)
- [`TS.MDEL`](commands/ts.mdel.md)
- [`TS.MGET`](commands/ts.mget.md)
- [`TS.METRICNAMES`](commands/ts.metricnames.md)
- [`TS.MRANGE`](commands/ts.mrange.md)
- [`TS.MREVRANGE`](commands/ts.mrevrange.md)
- [`TS.NRANGE`](commands/ts.nrange.md)
- [`TS.NREVRANGE`](commands/ts.nrevrange.md)
- [`TS.OUTLIERS`](commands/ts.outliers.md)
- [`TS.QUERYINDEX`](commands/ts.queryindex.md)
- [`TS.QUERYLABELS`](commands/ts.querylabels.md)
- [`TS.RANGE`](commands/ts.range.md)
- [`TS.READ`](commands/ts.read.md)
- [`TS.REVRANGE`](commands/ts.revrange.md)
- [`TS._DEBUG`](commands/ts._debug.md)
