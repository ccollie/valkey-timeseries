# Time-Series EDA with Valkey-TimeSeries and Python

A hands-on tour of the module's analytical commands, walking through a complete
**exploratory data analysis (EDA)** lifecycle for a single metric — from raw
ingestion to a forecast — using the [`valkey-py`](https://github.com/valkey-io/valkey-py)
client.

The goal is to show *what analysis you can push into the database* instead of
pulling every sample into pandas. Valkey-TimeSeries is a compute engine as much
as a store: feature extraction, decomposition, anomaly detection, stationarity
testing, and imputation all run server-side, close to the data (the heavier
analyses move large ranges onto a dedicated worker pool). Python's job in this
tutorial is orchestration and visualization.

---

## Table of contents

1. [What the module covers — and where Python takes over](#1-what-the-module-covers--and-where-python-takes-over)
2. [Setup: connection and helpers](#2-setup-connection-and-helpers)
3. [Build and load a dataset](#3-build-and-load-a-dataset)
4. [Profile the series (`TS.STATS`, `TS.INFO`)](#4-profile-the-series)
5. [Missing data — gaps (`TS.FILLGAPS`)](#5-missing-data--detecting-gaps)
6. [Missing data — imputation (`TS.SANITIZE`)](#6-missing-data--imputation)
7. [Feature analysis (`TS.FEATURES`)](#7-feature-analysis)
8. [Autocorrelation (`TS.AUTOCORRELATION`)](#8-autocorrelation)
9. [Seasonality detection (`TS.PERIODS`)](#9-seasonality-detection)
10. [Decomposition (`TS.DECOMPOSE`)](#10-decomposition)
11. [Trend analysis (`TS.TREND`)](#11-trend-analysis)
12. [Stationarity testing (`TS.STATIONARITY`)](#12-stationarity-testing)
13. [Anomaly detection and cleaning (`TS.OUTLIERS`)](#13-anomaly-detection-and-cleaning)
14. [Capstone: forecasting (`TS.AUTOFORECAST`)](#14-capstone-forecasting)
15. [Bonus: relationships between series (`TS.JOIN`)](#15-bonus-relationships-between-series)
16. [Cheat sheet](#16-cheat-sheet)

> **Illustrative output.** Numeric results shown below depend on the synthetic
> data and on the module's internal algorithms; treat every printed value as
> representative, not exact. What matters is the *shape* of each reply and how to
> interpret it.

---

## 1. What the module covers — and where Python takes over

Every stage of a standard time-series EDA maps onto one or more commands:

| EDA stage | In the database | Your Python code |
|---|---|---|
| Ingest & store | `TS.CREATE`, `TS.ADD` / `TS.MADD` / `TS.ADDBULK` | Collect/generate samples |
| Profile / describe | `TS.STATS`, `TS.INFO` | Read the map, branch on quality flags |
| Missing data — gaps | `TS.FILLGAPS` | Report / visualize gap timestamps |
| Missing data — NaN/Inf | `TS.SANITIZE` (11 policies) | Choose an imputation policy |
| Feature extraction | `TS.FEATURES` | Assemble a feature vector |
| Autocorrelation | `TS.AUTOCORRELATION` (ACF/PACF/TRA) | Build ACF / PACF plots |
| Seasonality | `TS.PERIODS` | Feed periods to downstream steps |
| Decomposition | `TS.DECOMPOSE` (STL / MSTL) | Plot trend / seasonal / residual |
| Trend | `TS.TREND` (auto model selection by AICc/BIC/holdout) | Compare models, project |
| Stationarity | `TS.STATIONARITY` (ADF + KPSS) | Decide whether to difference |
| Anomaly detection | `TS.OUTLIERS` (10 methods) | Annotate / alert |
| Cleaning | `TS.OUTLIERS OUTPUT CLEANED`, `TS.SANITIZE` | Persist cleaned series |
| Forecasting | `TS.AUTOFORECAST`, `TS.FORECAST`, `TS.BACKTEST` | Consume forecast + intervals |
| Cross-series | `TS.JOIN` (INNER / ASOF / REDUCE), `TS.XCORR` | Align, correlate, and lead/lag two series |

### Where the module stops

The module is a numerical engine, not a notebook. Four things it deliberately
does **not** do, which you handle client-side:

1. **Visualization.** Commands return arrays and maps; there are no plots. Pull a
   component into `matplotlib`/`pandas` when you want to *see* it. Every plotting
   snippet below is optional.
2. **Transforms (differencing, log, scaling).** `TS.STATIONARITY` tells you
   *whether* a series needs differencing; it does not difference it for you.
   Apply the transform in Python and load the result into a new key (Section 12
   does exactly that).
3. **Histograms / full empirical distribution.** `TS.STATS` gives skewness and
   kurtosis, `TS.FEATURES` gives quantiles, but binning is on you (from
   `TS.RANGE`).
4. **Feature matrices across many series at once.** `TS.FEATURES` is per-series;
   loop over keys returned by `TS.QUERYINDEX` to build a matrix.

Cross-correlation *used* to be on this list — `TS.XCORR` (Section 15) now computes
the full lead/lag correlation curve between two timestamp-aligned series
server-side, the same way `TS.AUTOCORRELATION` does for a series against itself.

Keep this table in mind: the tutorial front-loads the server-side work and only
drops to Python for the items above.

---

## 2. Setup: connection and helpers

```bash
pip install valkey          # the Python client
pip install matplotlib      # optional, only for the plot snippets
```

Start a Valkey server with the module loaded (see the project README), then
connect. We negotiate **RESP3** (`protocol=3`) so that map-returning commands
(`TS.INFO`, `TS.STATS`, `TS.FEATURES`, `TS.STATIONARITY`, `TS.TREND`,
`TS.DECOMPOSE`, `TS.AUTOFORECAST`, `TS.XCORR`) come back as native Python `dict`s.

```python
import json
import math
import random

from valkey import Valkey

client = Valkey(host="localhost", port=6379, protocol=3, decode_responses=True)
client.ping()  # -> True
```

A few small helpers normalize replies so the rest of the tutorial reads cleanly.
They are written to work under **both** RESP3 and RESP2, so you can drop
`protocol=3` and they still behave.

```python
def as_map(reply):
    """A TS.* map reply is a dict under RESP3 or a flat [k, v, k, v, ...]
    list under RESP2. Return a dict either way."""
    if isinstance(reply, dict):
        return reply
    it = iter(reply)
    return dict(zip(it, it))


def num(x):
    """Coerce a scalar (str under RESP2, float/int under RESP3) to float.
    Understands 'nan', 'inf', '-inf'."""
    if x is None:
        return float("nan")
    if isinstance(x, (int, float)):
        return float(x)
    s = str(x).strip().lower()
    if s in ("nan", "-nan"):
        return float("nan")
    if s in ("inf", "+inf"):
        return float("inf")
    if s == "-inf":
        return float("-inf")
    return float(x)


def samples(reply):
    """Turn an array of [timestamp, value] pairs into (int, float) tuples."""
    return [(int(ts), num(v)) for ts, v in reply]
```

---

## 3. Build and load a dataset

We synthesize **8 weeks of hourly** demand for a fictional metric,
`energy:demand`, so the tutorial is fully reproducible. The signal is
deliberately rich enough to exercise every command:

* a slow upward **trend**,
* a **daily** cycle (period = 24) and a **weekly** cycle (period = 168),
* Gaussian **noise**,
* a handful of injected **anomalies** (spikes and dips),
* some **missing timestamps** (gaps), and
* a few **NaN** values (present but unmeasured).

```python
random.seed(7)

HOUR = 3_600_000                     # one hour in milliseconds
START = 1_704_067_200_000            # 2024-01-01T00:00:00Z
N = 24 * 7 * 8                       # 8 weeks of hourly points = 1344

timestamps, values = [], []
for i in range(N):
    hour_of_day = i % 24
    day_of_week = (i // 24) % 7
    trend  = 50 + 0.02 * i
    daily  = 12 * math.sin(2 * math.pi * hour_of_day / 24)
    weekly = 6  * math.sin(2 * math.pi * day_of_week / 7)
    noise  = random.gauss(0, 2.0)
    timestamps.append(START + i * HOUR)
    values.append(round(trend + daily + weekly + noise, 3))

# Inject anomalies (index -> additive shock).
for idx, shock in [(300, +45), (620, -38), (911, +52), (1180, -40)]:
    values[idx] = round(values[idx] + shock, 3)

# Designate gaps (dropped timestamps) and NaNs (present-but-missing values),
# keeping the two index sets disjoint.
gap_idx = set(range(500, 512)) | {700, 701, 702}      # 15 missing hours
nan_idx = {120, 121, 340, 905}                         # 4 NaN readings
```

Load the finite samples in bulk with `TS.ADDBULK` (JSON payload, up to 1000
samples per call), then add the NaN readings individually with `TS.ADD`. We
create the series first so it carries labels for later discovery.

```python
KEY = "energy:demand"
client.delete(KEY)
client.execute_command(
    "TS.CREATE", KEY,
    "LABELS", "metric", "energy_demand", "unit", "kwh", "site", "hq",
)

def add_bulk(key, ts_list, val_list, chunk=1000):
    total = 0
    for i in range(0, len(ts_list), chunk):
        payload = json.dumps({
            "timestamps": ts_list[i:i + chunk],
            "values":     val_list[i:i + chunk],
        })
        ingested, _seen = client.execute_command("TS.ADDBULK", key, payload)
        total += int(ingested)
    return total

finite_ts  = [t for i, t in enumerate(timestamps) if i not in gap_idx | nan_idx]
finite_val = [v for i, v in enumerate(values)     if i not in gap_idx | nan_idx]
print("ingested:", add_bulk(KEY, finite_ts, finite_val))   # -> 1325

for i in sorted(nan_idx):
    client.execute_command("TS.ADD", KEY, timestamps[i], "nan")
```

Quick sanity check with `TS.INFO` and `TS.GET`:

```python
info = as_map(client.execute_command("TS.INFO", KEY))
print("total samples:", info.get("totalSamples"))
print("first / last :", info.get("firstTimestamp"), info.get("lastTimestamp"))
print("latest sample:", client.execute_command("TS.GET", KEY))
```

You now have one raw series with realistic warts. The rest of the tutorial
profiles it, cleans it, and analyzes it.

---

## 4. Profile the series

Start every EDA with a one-shot profile. `TS.STATS` returns descriptive
statistics **and** data-quality indicators in a single map — central tendency,
dispersion, distribution shape, and counts of zeros / NaNs / leading-trailing
zeros / plateaus.

```python
stats = as_map(client.execute_command("TS.STATS", KEY))

print(f"length     : {int(stats['length'])}")
print(f"mean / std : {num(stats['mean']):.2f} / {num(stats['std']):.2f}")
print(f"min / max  : {num(stats['min']):.2f} / {num(stats['max']):.2f}")
print(f"median     : {num(stats['median']):.2f}")
print(f"skewness   : {num(stats['skewness']):.3f}")
print(f"kurtosis   : {num(stats['kurtosis']):.3f}")
print(f"NaN / Inf  : {int(stats['n_nans'])}")        # -> 4, the readings we flagged
print(f"unique     : {int(stats['n_unique_values'])}")
```

```text
length     : 1329
mean / std : 63.51 / 12.33
min / max  : 9.31 / 123.12
median     : 63.36
skewness   : 0.029
kurtosis   : 0.034
NaN / Inf  : 4
unique     : 1300
```

**How to read it.** `n_nans = 4` immediately tells you the series has missing
values to deal with (Section 6). A large `range` relative to `std`, or a
`skewness`/`kurtosis` far from 0, hints at outliers (Section 13). `is_constant`,
`plateau_size`, and `n_zeros_start`/`n_zeros_end` are cheap sensor-health flags —
a long plateau or a run of leading zeros usually means a stuck or not-yet-online
sensor. You can scope the profile to any window: `TS.STATS energy:demand
<from> <to>`.

---

## 5. Missing data — detecting gaps

There are two distinct kinds of "missing" in time series, and the module
separates them:

* **Gaps** — timestamps that *should* exist on the sampling grid but don't.
* **NaN/Inf** — samples that exist but carry no valid value.

`TS.FILLGAPS` finds the first kind. It infers the sampling frequency (or you pass
`FREQUENCY`), computes which grid timestamps are missing, and returns them. It
**never modifies the source series** — `STORE` writes to a different key.

```python
gaps = samples(client.execute_command(
    "TS.FILLGAPS", KEY, "-", "+", "FREQUENCY", "1h",
))
print(f"detected {len(gaps)} missing hours")        # -> 15
for ts, _v in gaps[:5]:
    print("  gap at", ts)
```

The returned value for each gap is `NaN` by default; pass `VALUE 0` to mark them
with a sentinel instead. `STORE` writes *only the gap rows*, and without `MERGE` it
clears the destination first. So to build a complete hourly grid, copy the raw
series (Valkey's `COPY` works on time-series keys, labels included) and merge the
gap rows into the copy:

```python
GRID = "energy:demand:gridded"
client.delete(GRID)
client.copy(KEY, GRID)                              # working copy of the raw data
written = client.execute_command(
    "TS.FILLGAPS", KEY, "-", "+", "FREQUENCY", "1h",
    "VALUE", "nan", "STORE", GRID, "MERGE",
)
print("gap rows written:", written)                 # -> 15
```

Now `energy:demand:gridded` holds all 1344 hourly slots, 19 of them NaN (the 15
gaps plus the 4 NaN readings), ready for imputation. `TS.FILLGAPS` is the *detection* half; `TS.SANITIZE` is the
*repair* half.

---

## 6. Missing data — imputation

`TS.SANITIZE` repairs NaN/Inf values within a range using one of **11 policies**.
It defaults to `DROP`.

> **It always rewrites its source.** With or without `STORE`, the sanitized
> samples replace the range in the source series; `STORE` *additionally* writes
> them to a separate key (and replies with a count instead of the samples). There
> is no dry run, so point it at a working copy — never at the raw key you want to
> keep.

To preview a policy, run it on a throwaway copy of the grid. Without `STORE` it
returns the sanitized samples inline — here, the start of the 12-hour gap:

```python
client.copy(GRID, "energy:demand:scratch", replace=True)
preview = samples(client.execute_command(
    "TS.SANITIZE", "energy:demand:scratch", "-", "+", "POLICY", "INTERPOLATE",
))
print("gap, interpolated:", preview[500:503])
```

The policy you pick depends on the data's character:

| Policy | Best for |
|---|---|
| `INTERPOLATE` | Smooth signals with short gaps (linear between neighbors) |
| `FORWARDFILL` / `BACKWARDFILL` | Step-like signals (last/next observation carried) |
| `FORWARDBACKWARDFILL` | Handles interior *and* edge NaNs |
| `FILLMEAN` / `FILLMEDIAN` | Stationary signals with no local structure |
| `FILL value` | A known constant (e.g. `0` for "no events") |
| `MOVINGAVERAGE window` | Noisy signals (centered window mean; window odd) |
| `SEASONAL period\|auto` | Strong periodicity — fill from same phase in the cycle |
| `DROP` / `ERROR` | Discard, or fail loudly if any NaN is present |

Our demand series is seasonal, so `SEASONAL` (borrow the same hour from other
days) is a natural choice. Sanitize the grid and `STORE` the result into a
dedicated analysis key (the grid is repaired in place too; the raw
`energy:demand` is never touched):

```python
CLEAN = "energy:demand:clean"
client.delete(CLEAN)
written = client.execute_command(
    "TS.SANITIZE", GRID, "-", "+",
    "POLICY", "SEASONAL", "24",      # the daily cycle; Section 9 confirms it
    "STORE", CLEAN,
)
print("clean samples written:", written)                    # -> 1344

# Confirm the clean copy has zero NaNs.
clean_stats = as_map(client.execute_command("TS.STATS", CLEAN))
print("NaN after sanitize:", int(clean_stats["n_nans"]))   # -> 0
```

> **Guard `SEASONAL`.** It errors if more than 50% of a seasonal bucket is
> missing. `SEASONAL auto` detects the period from the range itself and currently
> fails (`unable to detect dominant period`) whenever that range still contains a
> NaN — so name the period explicitly. On sparse data, fall back to `INTERPOLATE`
> or `FORWARDBACKWARDFILL`.

From here on, **analysis runs against `energy:demand:clean`** — a complete,
NaN-free hourly grid. Imputation only touched the missing slots, so the injected
anomalies are still in it for Section 13 to find.

---

## 7. Feature analysis

`TS.FEATURES` extracts named statistical features (a tsfresh-compatible set) in
one pass. Request them by **category** or **individually**; the reply is a
`{feature_name: value}` map. Under RESP3 that's a dict directly.

```python
feats = as_map(client.execute_command(
    "TS.FEATURES", CLEAN, "-", "+",
    "CATEGORY", "basic,distribution,trend",
))
for name in sorted(feats):
    print(f"{name:28s} {num(feats[name]):.4f}")
```

```text
kurtosis                     0.0397
length                       1344.0000
linear_trend_intercept       51.1952
linear_trend_p_value         0.0000
linear_trend_r_squared       0.3381
linear_trend_slope           0.0184
maximum                      123.1230
mean                         63.5580
...
skewness                     0.0189
variance                     150.8891
variance_sample              151.0015
```

The four categories are `basic`, `distribution`, `autocorrelation`, and `trend`.
You can also name features directly, including **parameterized** ones with a
`name:value` syntax — handy for building a fixed feature vector for an ML model:

```python
vec = as_map(client.execute_command(
    "TS.FEATURES", CLEAN, "-", "+",
    "FEATURE", "mean,standard_deviation,quantile:0.95,"
               "autocorrelation:24,pacf:1,augmented_dickey_fuller",
))
print({k: round(num(v), 4) for k, v in vec.items()})
```

`quantile:0.95` becomes the key `quantile_0.95`, `autocorrelation:24` becomes
`autocorrelation_24`, and `pacf:1` becomes `partial_autocorrelation_1`. Features that evaluate to NaN come back as `null`
(→ `float('nan')` via our `num` helper). This single call is the fastest way to
turn a raw series into a row of model-ready numbers without transferring any
samples to the client.

---

## 8. Autocorrelation

Autocorrelation reveals memory and periodicity. `TS.AUTOCORRELATION` computes
four related statistics at a given lag:

* **ACF** (default) — correlation with the series lagged by `k`.
* **PACF** (`PARTIAL`) — the same, with the effect of shorter lags removed
  (Durbin–Levinson). This is what you read to choose an AR order.
* **TRA** (`TRA`) — time-reversal asymmetry, a non-linearity signature.
* **AGGREGATED** `mean|var|std|median` — summarize ACF across lags `1..=k`.

```python
def acf(key, lag, partial=False):
    args = ["TS.AUTOCORRELATION", key, "-", "+", lag]
    if partial:
        args.append("PARTIAL")
    return num(client.execute_command(*args))

# ACF at the daily lag should stand out sharply for hourly data.
for lag in (1, 6, 12, 24, 48, 168):
    print(f"lag {lag:3d}:  acf={acf(CLEAN, lag):+.3f}")
```

```text
lag   1:  acf=+0.913
lag   6:  acf=+0.452
lag  12:  acf=-0.024
lag  24:  acf=+0.874     <- daily cycle
lag  48:  acf=+0.774
lag 168:  acf=+0.820     <- weekly cycle
```

The peaks at lag 24 and lag 168 confirm the daily and weekly seasonality you'll
formalize in Section 9. Since the module returns one scalar per call, build an
**ACF plot** by looping — the classic correlogram:

```python
# Optional: correlogram of ACF and PACF.
import matplotlib.pyplot as plt

lags = list(range(1, 49))
acf_vals  = [acf(CLEAN, k) for k in lags]
pacf_vals = [acf(CLEAN, k, partial=True) for k in lags]

fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(10, 5), sharex=True)
ax1.stem(lags, acf_vals);  ax1.set_ylabel("ACF")
ax2.stem(lags, pacf_vals); ax2.set_ylabel("PACF"); ax2.set_xlabel("lag")
fig.suptitle("energy:demand:clean — correlogram")
plt.tight_layout(); plt.savefig("correlogram.png")
```

Aggregated variants collapse the correlogram to a single diagnostic — e.g. the
mean ACF over the first day is a quick "how autocorrelated is this, overall?":

```python
mean_acf = num(client.execute_command(
    "TS.AUTOCORRELATION", CLEAN, "-", "+", 24, "AGGREGATED", "mean",
))
print("mean ACF over lags 1..24:", round(mean_acf, 3))
```

---

## 9. Seasonality detection

Rather than guessing the period, ask the data. `TS.PERIODS` uses a spectral
(SAZED) ensemble to detect periodic structure. Each detected period comes back as
`[period, power, strength, acf, n_cycles]`.

```python
periods = client.execute_command("TS.PERIODS", CLEAN, "-", "+")
for period, power, strength, ac, n_cycles in periods:
    print(f"period={int(period):4d}  strength={num(strength):.2f}  "
          f"acf={num(ac):.2f}  cycles={int(n_cycles)}")
```

```text
period= 168  strength=0.80  acf=0.82  cycles=8      <- weekly
period=  24  strength=0.66  acf=0.87  cycles=56     <- daily
```

`strength` is the actionable number: `> 0.6` is strong seasonality, `< 0.3` is
weak. For a quick one-liner — "what's the single dominant cycle?" — use
`DOMINANT`, which returns a bare integer (or `nil`). It runs its own single-period
search, so it need not match the first row above:

```python
dominant = client.execute_command("TS.PERIODS", CLEAN, "-", "+", "DOMINANT")
print("dominant period:", dominant)                 # -> 24
```

The detected periods feed directly into the next three commands: decomposition,
seasonality-aware outlier detection, and seasonal forecasting all take a period
argument.

---

## 10. Decomposition

`TS.DECOMPOSE` splits the series into **trend + seasonal + residual** using STL
(one period) or MSTL (multiple periods). Pass `SEASONALITY auto` to let it detect
periods, or name them explicitly. The reply is a flat array of component name →
`[ts, value]` samples (`as_map` turns it into a dict) satisfying
`original = trend + seasonal + residual`.

Give it both cycles we found (MSTL):

```python
decomp = as_map(client.execute_command(
    "TS.DECOMPOSE", CLEAN, "-", "+", "SEASONALITY", "24", "168",
))
print("components:", list(decomp.keys()))
# -> ['original', 'trend', 'seasonal_components', 'residual']

trend    = samples(decomp["trend"])
residual = samples(decomp["residual"])
print("trend start -> end:", round(trend[0][1], 1), "->", round(trend[-1][1], 1))
```

With a **single** period the key is `seasonal`; with **multiple** periods it's
`seasonal_components`, a list of `[period, [[ts, value], ...]]`. Plot the panels
to see the structure the earlier commands quantified:

```python
# Optional: classic decomposition panel plot.
import matplotlib.pyplot as plt

original = samples(decomp["original"])
ts   = [t for t, _ in original]
fig, axes = plt.subplots(3, 1, figsize=(11, 6), sharex=True)
axes[0].plot(ts, [v for _, v in original]); axes[0].set_ylabel("original")
axes[1].plot(ts, [v for _, v in trend]);    axes[1].set_ylabel("trend")
axes[2].plot(ts, [v for _, v in residual]); axes[2].set_ylabel("residual")
plt.tight_layout(); plt.savefig("decomposition.png")
```

The **residual** is the useful by-product: once trend and seasonality are removed,
what's left should look like noise. Systematic structure in the residual means
your period choice was wrong; large isolated spikes are candidate anomalies —
which is exactly why `TS.OUTLIERS` can decompose first (Section 13).

---

## 11. Trend analysis

`TS.TREND` fits and returns the trend component. In **Auto** mode (default) it
fits a set of candidate models — Linear, Quadratic, Exponential, Logistic,
TheilSen, PiecewiseLinear — and selects one by an information criterion (AICc by
default, or BIC / HOLDOUT).

```python
trend = as_map(client.execute_command(
    "TS.TREND", CLEAN, "-", "+", "METRICS",
))
print("selected model:", trend["model"])            # -> e.g. "Linear"
print("criterion     :", trend["criterion"])        # -> "AICc"

metrics = as_map(trend["accuracy_metrics"])
print("R^2  :", round(num(metrics["r_squared"]), 4))
print("RMSE :", round(num(metrics["rmse"]), 4))

scores = [(name, num(s)) for name, s in trend["scores"]]
print("model ranking (lower is better):")
for name, s in scores:
    print(f"  {name:16s} {s:.2f}")
```

Three flags make it an EDA workhorse:

* `FEATURES` — adds a `features` map describing the fitted component (e.g.
  `theilsen_slope`).
* `METRICS` — adds `accuracy_metrics` (MAE, RMSE, MAPE, sMAPE, MASE, R²) of
  observed vs. fitted.
* `PREDICT n` — projects the trend `n` steps ahead into `predicted_trend`.

`RECENCY` controls *which* data drives the fit — often you care about the recent
regime, not ancient history (the default is the last 30%). And `STORE` writes the fitted (and predicted)
trend to a key so you can overlay it with `TS.RANGE`:

```python
client.execute_command(
    "TS.TREND", CLEAN, "-", "+",
    "MODEL", "TheilSen",          # robust to the anomalies still in the data
    "RECENCY", "FRACTION", "0.5", # fit on the most recent 50%
    "PREDICT", "24",              # project one day ahead
    "STORE", "energy:demand:trend",
)
overlay = samples(client.execute_command("TS.RANGE", "energy:demand:trend", "-", "+"))
print("stored trend points:", len(overlay))          # -> 1368 (1344 fitted + 24)
```

TheilSen is the robust choice here because the anomalies still in the data
would drag a least-squares line; the median-of-slopes estimator largely ignores
them.

---

## 12. Stationarity testing

Many models assume **stationarity** — constant mean/variance/autocorrelation over
time. `TS.STATIONARITY` runs ADF and KPSS (complementary tests) and, in the
default `combined` mode, returns a single verdict plus both tests' details.

```python
st = as_map(client.execute_command("TS.STATIONARITY", CLEAN, "-", "+"))
print("verdict:", st["conclusion"])                 # stationary | non_stationary | inconclusive

adf  = as_map(st["adf"])
kpss = as_map(st["kpss"])
print(f"ADF : stat={num(adf['statistic']):.3f}  p={num(adf['pValue']):.3f}  "
      f"stationary={int(adf['isStationary'])}")
print(f"KPSS: stat={num(kpss['statistic']):.3f}  p={num(kpss['pValue']):.3f}  "
      f"stationary={int(kpss['isStationary'])}")
```

```text
verdict: inconclusive
ADF : stat=-7.818  p=0.001  stationary=1
KPSS: stat=7.382  p=0.000  stationary=0
```

The tests disagree. ADF rejects a unit root — the hourly swings keep reverting —
but KPSS rejects a constant mean, because the series trends upward. That split
is the classic signature of a trend, and the verdict is **inconclusive**. The
module diagnoses this but leaves the remedy to you — **differencing is a client-side
transform** (one of the gaps from Section 1). Difference in Python, write the
result to a new key, and re-test:

```python
raw = samples(client.execute_command("TS.RANGE", CLEAN, "-", "+"))
diff_ts  = [raw[i][0] for i in range(1, len(raw))]
diff_val = [raw[i][1] - raw[i - 1][1] for i in range(1, len(raw))]

client.delete("energy:demand:diff")
add_bulk("energy:demand:diff", diff_ts, diff_val)   # reuse the loader from Section 3

st2 = as_map(client.execute_command("TS.STATIONARITY", "energy:demand:diff", "-", "+"))
print("after differencing:", st2["conclusion"])     # -> stationary
```

The `combined` verdict resolves the common textbook confusion: ADF's null is
"non-stationary" while KPSS's null is "stationary", so they answer the question
from opposite directions. When they agree you get a clean `stationary` /
`non_stationary`; when they disagree you get `inconclusive` — a signal to inspect
the series (a deterministic trend, as here, or heteroskedastic variance) rather
than trust one test.

---

## 13. Anomaly detection and cleaning

`TS.OUTLIERS` runs against `energy:demand:clean`: imputation left the injected
shocks in place, and seasonal adjustment (below) needs exactly what that key
provides — a regular grid with no NaNs. (On a range that still contains a NaN,
`SEASONALITY` currently scores every sample `0` and reports nothing.) It offers
ten methods — statistical (`ZSCORE`, `MODIFIED-ZSCORE`, `IQR`, `MAD`,
`DOUBLE-MAD`, `ESD`), control-chart (`CUSUM`, `EWMA`, `SMOOTHED-ZSCORE`), and ML
(`RCF`, Random Cut Forest) — plus three output modes.

Detect first (`SIMPLE` returns `[timestamp, value, signal, score]` per anomaly;
`score` is on a common 0–1 scale for every method, and anything above `0.5` was
flagged):

```python
anoms = client.execute_command(
    "TS.OUTLIERS", CLEAN, "-", "+", "METHOD", "MODIFIED-ZSCORE", "THRESHOLD", "3.5",
)
print(f"{len(anoms)} anomalies")
for ts, value, signal, score in anoms:
    direction = "spike" if int(signal) > 0 else "dip"
    print(f"  {ts}  {num(value):8.2f}  {direction:5s}  score={num(score):.2f}")
```

```text
2 anomalies
  1706299200000      9.31  dip    score=0.55
  1707346800000    123.12  spike  score=0.57
```

Only two of the four injected shocks clear a global threshold: a value can be
perfectly normal *for 3 a.m.* yet look low against the global median, and the
daily/weekly swing widens the spread enough to hide smaller shocks. Add
`SEASONALITY` so detection runs on the **de-seasonalized residual**, catching
context-aware anomalies and ignoring the swing:

```python
anoms_seasonal = client.execute_command(
    "TS.OUTLIERS", CLEAN, "-", "+",
    "SEASONALITY", "24", "168",          # remove daily + weekly first
    "METHOD", "ZSCORE", "THRESHOLD", "3.0",
    "DIRECTION", "BOTH",
)
print("context-aware anomalies:", len(anoms_seasonal))     # -> 7
```

All four shocks are among the seven. The other three are the NaN readings
`SEASONAL` imputed in Section 6: a flat per-hour median ignores the trend and the
weekly phase, so the filled values stand out once both are removed — a useful
check on your imputation.

`OUTPUT FULL` returns every sample with its score plus method metadata (e.g. IQR
fences, control limits) — useful for thresholded dashboards. And `OUTPUT CLEANED`
turns detection into **cleaning**, returning only the normal samples:

```python
cleaned = samples(client.execute_command(
    "TS.OUTLIERS", CLEAN, "-", "+", "OUTPUT", "CLEANED", "METHOD", "IQR", "THRESHOLD", "1.5",
))
print("kept", len(cleaned), "of", 1344, "samples")        # -> kept 1342 of 1344
```

### The full clean pipeline

Combining Sections 5, 6, and 13 gives the analysis-ready series. Note
`TS.OUTLIERS OUTPUT CLEANED` *removes* anomalous points (creating gaps), which is
why gap-fill/impute comes after it. `TS.FILLGAPS` can't `STORE` into its own
source, and JSON has no NaN for `TS.ADDBULK`, so the holes go back in with one
`TS.MADD`:

```python
# 1) Drop outliers → 2) put the holes back as NaN → 3) impute them in place.
ANALYSIS = "energy:demand:analysis"
inliers = samples(client.execute_command(
    "TS.OUTLIERS", CLEAN, "-", "+", "OUTPUT", "CLEANED",
    "SEASONALITY", "24", "168", "METHOD", "ZSCORE", "THRESHOLD", "3.0",
))
client.delete(ANALYSIS)
add_bulk(ANALYSIS, [t for t, _ in inliers], [v for _, v in inliers])

holes = client.execute_command("TS.FILLGAPS", ANALYSIS, "-", "+", "FREQUENCY", "1h")
if holes:
    client.execute_command("TS.MADD", *[x for ts, _ in holes for x in (ANALYSIS, ts, "nan")])

client.execute_command("TS.SANITIZE", ANALYSIS, "-", "+",    # rewrites ANALYSIS
                       "POLICY", "SEASONAL", "24")
print("analysis-ready:", int(as_map(
    client.execute_command("TS.STATS", ANALYSIS))["n_nans"]), "NaNs")   # -> 0
```

---

## 14. Capstone: forecasting

EDA earns its keep when it feeds a model. `TS.AUTOFORECAST` evaluates several
model families (AutoARIMA, AutoETS, AutoTheta by default; TBATS/MFLES/MSTL
opt-in) by cross-validation and returns the best forecast. Everything you learned
above informs the call: the dominant period from Section 9 becomes `SEASONALITY`.

```python
fc = as_map(client.execute_command(
    "TS.AUTOFORECAST", "energy:demand:analysis", "-", "+",
    "HORIZON", "48",           # two days ahead
    "SEASONALITY", "24",       # from TS.PERIODS DOMINANT
    "LEVEL", "95",             # 95% prediction intervals
    "METRICS",
))

print("selected model:", fc["model"])
forecast = [num(x) for x in fc["forecast"]]
lower    = [num(x) for x in fc["lower_interval"]]
upper    = [num(x) for x in fc["upper_interval"]]
print("first 3 forecasts:", [round(x, 1) for x in forecast[:3]])
print("first interval    :", round(lower[0], 1), "..", round(upper[0], 1))

metrics = as_map(fc["metrics"])
print("in-sample RMSE:", round(num(metrics["rmse"]), 3))
```

```text
selected model: ETS
first 3 forecasts: [70.5, 73.6, 76.5]
first interval    : 65.5 .. 75.6
in-sample RMSE: 2.537
```

`METRICS` scores the winning model's in-sample fit. For a differencing model
(ARIMA or SARIMA) that fit is rebuilt on the original scale from its one-step
residuals, so the numbers are comparable across families; for an out-of-sample
view, evaluate with `TS.BACKTEST`.

Add `STORE energy:demand:forecast` to persist the forecast as a real series (its
timestamps continue from the last observed point, one sampling interval apart),
so you can query it with `TS.RANGE` or chart it beside the history. For a single
named model use `TS.FORECAST`; to evaluate forecast accuracy over rolling origins
use `TS.BACKTEST`.

---

## 15. Bonus: relationships between series

Univariate EDA answers "how does this metric behave?" Two commands extend it to
"how do two metrics relate?":

* **`TS.JOIN`** time-aligns two series (INNER by default; also
  LEFT/RIGHT/FULL/ANTI/SEMI and, crucially for time series, **ASOF** for
  nearest-timestamp matching) and can combine each aligned pair with a `REDUCE`
  operator — arithmetic between two series at matching timestamps.
* **`TS.XCORR`** computes the full **cross-correlation function (CCF)** between
  two series — the two-series counterpart of `TS.AUTOCORRELATION` from Section 8.
  It handles the timestamp alignment internally (the same inner join `TS.JOIN`
  performs by default) and returns the Pearson correlation at every lag in
  `-maxLag..=maxLag` in one call, entirely server-side.

Suppose you also track outdoor `weather:temp` and want to know whether it *leads*
`energy:demand` (a cold snap now predicts heating demand a few hours later) or
lags it. For the tutorial we synthesize a temperature series on the same hourly
grid that runs opposite to demand's swings, three hours ahead of them:

```python
TEMP = "weather:temp"
client.delete(TEMP)
temp_ts, temp_val = [], []
for i in range(N - 3):
    swing = values[i + 3] - (50 + 0.02 * (i + 3))   # demand 3 h later, detrended
    temp_ts.append(timestamps[i])
    temp_val.append(round(12 - 0.5 * swing + random.gauss(0, 1.5), 2))
add_bulk(TEMP, temp_ts, temp_val)
```

Now ask the data which way round it is with `TS.XCORR`:

```python
xcorr = as_map(client.execute_command(
    "TS.XCORR", TEMP, "energy:demand:analysis", "-", "+", 48,
))

lags   = [int(x) for x in xcorr["lags"]]
values = [num(x) for x in xcorr["values"]]

print(f"peak lag: {int(xcorr['peak_lag'])}  "
      f"peak correlation: {num(xcorr['peak_correlation']):+.3f}  "
      f"(n={int(xcorr['n'])} aligned pairs)")
```

```text
peak lag: 3  peak correlation: -0.709  (n=1341 aligned pairs)
```

**Reading the lag.** For key1=`weather:temp`, key2=`energy:demand:analysis`, and
lag `h`: `weather:temp[t]` is compared against `energy:demand[t + h]` (lags count
samples, so hours here). `peak_lag` is the lag with the largest *absolute*
correlation. A peak at `h = +3` means **`weather:temp` leads `energy:demand` by 3
hours**, and the negative sign says the relationship is inverse: colder now,
higher demand three hours on. Swap the key order if you'd rather read the sign
the other way; the curve is the mirror image.

Plot the full CCF as a correlogram, exactly like the ACF/PACF plot in Section 8 —
except this time the module computed every point, not just one lag per call:

```python
# Optional: cross-correlogram.
import matplotlib.pyplot as plt

fig, ax = plt.subplots(figsize=(10, 3))
ax.stem(lags, values)
ax.axvline(int(xcorr["peak_lag"]), color="red", linestyle="--", linewidth=1,
           label=f"peak lag = {int(xcorr['peak_lag'])}")
ax.set_xlabel("lag (hours)"); ax.set_ylabel("correlation")
ax.set_title("weather:temp  ×  energy:demand:analysis — cross-correlogram")
ax.legend()
plt.tight_layout(); plt.savefig("cross_correlogram.png")
```

`TS.XCORR` only uses samples with **matching timestamps** in both series (an
inner join). If your two series sit on different or offset grids, align them
first — `TS.FILLGAPS` plus `TS.SANITIZE` to put both on the same grid, or resample
both with `TS.RANGE ... AGGREGATION` and load the buckets into new keys.

`TS.JOIN` still earns its keep for anything beyond correlation — combining two
series pointwise (`sub`, `div`, `pct_change`, ...) to derive a new series, e.g. a
spread or ratio to feed back into `TS.STATS`/`TS.OUTLIERS`:

```python
spread = client.execute_command(
    "TS.JOIN", "energy:demand:analysis", TEMP, "-", "+",
    "ASOF", "NEAREST", "30m",   # match within 30 minutes
    "REDUCE", "sub",            # demand - temp per aligned pair
)
# `spread` is [[timestamp, reduced_value], ...] — store it (via a follow-up
# TS.ADDBULK) if you want to run TS.STATS/TS.OUTLIERS on the derived series.
```

---

## 16. Cheat sheet

| Question you're asking | Command | Key options |
|---|---|---|
| What does this series look like? | `TS.STATS` | window `from to` |
| Are timestamps missing? | `TS.FILLGAPS` | `FREQUENCY`, `VALUE`, `STORE ... MERGE` |
| Are values missing (NaN)? | `TS.SANITIZE` | `POLICY` (11 of them), `STORE` — rewrites the source too |
| Give me a feature vector | `TS.FEATURES` | `CATEGORY`, `FEATURE name:val` |
| How autocorrelated / periodic? | `TS.AUTOCORRELATION` | `PARTIAL`, `TRA`, `AGGREGATED` |
| What cycles exist? | `TS.PERIODS` | `MIN_STRENGTH`, `DOMINANT` |
| Split trend/seasonal/residual | `TS.DECOMPOSE` | `SEASONALITY auto\|p...` |
| What's the trend, and next? | `TS.TREND` | `MODEL`, `RECENCY`, `PREDICT`, `METRICS`, `STORE` |
| Do I need to difference? | `TS.STATIONARITY` | `TEST adf\|kpss\|combined` |
| Where are the anomalies? | `TS.OUTLIERS` | `METHOD` (10), `SEASONALITY`, `OUTPUT` |
| Forecast the future | `TS.AUTOFORECAST` | `HORIZON`, `SEASONALITY`, `LEVEL`, `METRICS`, `STORE` |
| Align / combine two series | `TS.JOIN` | `ASOF`, `REDUCE`, `AGGREGATION` |
| Does X lead or lag Y? | `TS.XCORR` | `maxLag`, read `peak_lag`/`peak_correlation` |

### The reusable helper module

Collected here for copy-paste:

```python
import json, math, random
from valkey import Valkey

client = Valkey(host="localhost", port=6379, protocol=3, decode_responses=True)

def as_map(reply):
    if isinstance(reply, dict):
        return reply
    it = iter(reply)
    return dict(zip(it, it))

def num(x):
    if x is None:
        return float("nan")
    if isinstance(x, (int, float)):
        return float(x)
    s = str(x).strip().lower()
    if s in ("nan", "-nan"):  return float("nan")
    if s in ("inf", "+inf"):  return float("inf")
    if s == "-inf":           return float("-inf")
    return float(x)

def samples(reply):
    return [(int(ts), num(v)) for ts, v in reply]
```

### See also

* Per-command references under [`docs/commands/`](../commands/) — every option, edge
  case, and error is documented there.
* [`docs/overview.md`](../overview.md) — architecture, indexing, and cluster behavior.
* [`docs/commands/ts.backtest.md`](../commands/ts.backtest.md) — evaluate forecast
  accuracy over rolling origins.
