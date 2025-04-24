### TS.CARD

#### Syntax

```
TS.CARD [START fromTimestamp] [END toTimestamp] [FILTER filter...]
```
returns the number of unique time series that match a certain label set.

### Required arguments

<details open><summary><code>filter</code></summary>
Repeated series selector argument that selects the series to return. Optional.
</details>

### Optional Arguments
<details open><summary><code>fromTimestamp</code></summary>
Start timestamp, inclusive. Results will only be returned for series which have samples in the range `[fromTimestamp, toTimestamp]`
</details>
<details open><summary><code>toTimestamp</code></summary>
End timestamp, inclusive.
</details>

#### Return

[Integer number](https://redis.io/docs/reference/protocol-spec#resp-integers) of unique time series.
The data section of the query result consists of a list of objects that contain the label name/value pairs which identify
each series.


#### Error

Return an error reply in the following cases:

TODO

#### Examples
TODO