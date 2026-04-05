# deltascan

<div>

[![Package version](https://img.shields.io/pypi/v/deltascan?color=%2334D058&label=pypi)](https://pypi.org/project/deltascan/)
[![License](https://img.shields.io/github/license/zteinck/deltascan)](https://github.com/zteinck/deltascan/blob/master/LICENSE)

</div>

`deltascan` is a Python package that finds and summarizes the differences between two datasets.

## Installation
```bash
pip install deltascan
```

## Main Features
The `DeltaScan` class compares any two supported data structures accross one or more dimensions.

#### Data Structures:
- `DataFrame`
- `Series`
- `LazyFrame` _(Polars only)_

#### Dimensions:
- `Rows` → rows present in one dataset but missing in the other, aligned using `join_on`.
- `Columns` → differences in column names and data types.
- `Values` → mismatched values within matching rows and columns.

## Example Usage

### Imports
Import the `DeltaScan` class.
```python
from deltascan import DeltaScan
```

### Create DataFrames
Create two sample `DataFrame` objects to compare.
```python
import pandas as pd
import polars as pl
import datetime


# February Data
left_data = pd.DataFrame({
    'id': [1, 2, 3, 4],
    'date': [pd.to_datetime('2026-02-28')] * 4,
    'first_name': ['Alice', 'Mike', 'John', 'Sarah'],
    'flag': [True, False, True, False],
    'amount': [10.0, 5.3, 33.7, 99.3],
    })

# January Data
right_data = pl.DataFrame({
    'id': [1, 3, 9],
    'date': [datetime.date(2026, 1, 31)] * 3,
    'first_name': ['Alice', 'Michael', 'Zachary'],
    'color': ['Pink', 'Blue', 'Red'],
    'last_name': ['Jones', 'Smith', 'Einck'],
    'flag': [False, True, False],
    'amount': [10, None, 14],
    })
```

### Compare DataFrames
Create a `DeltaScan` instance to perform the comparison. See the in-code documentation for a complete list of available arguments.
```python
ds = DeltaScan(
    left_data=left_data,
    right_data=right_data,
    join_on='id',
    left_alias='feb',
    right_alias='jan',
    left_context=['first_name'],
    right_context=None,
    verbose=True,
    )
```

### Comparison Results
Access the comparison results using the `summary` and `differences` attributes.
```python
print(ds.summary)
```
```
shape: (8, 6)
┌─────────────────────┬───────────┬─────────────┬─────────┬───────┬──────────────┐
│ Comparison          ┆ Dimension ┆ Differences ┆ Matches ┆ Total ┆ Match Rate % │
│ ---                 ┆ ---       ┆ ---         ┆ ---     ┆ ---   ┆ ---          │
│ str                 ┆ str       ┆ i64         ┆ i64     ┆ i64   ┆ f64          │
╞═════════════════════╪═══════════╪═════════════╪═════════╪═══════╪══════════════╡
│ jan cols not in feb ┆ columns   ┆ 2           ┆ 5       ┆ 7     ┆ 0.714286     │
│ data types          ┆ columns   ┆ 2           ┆ 3       ┆ 5     ┆ 0.6          │
│ feb rows not in jan ┆ rows      ┆ 2           ┆ 2       ┆ 4     ┆ 0.5          │
│ jan rows not in feb ┆ rows      ┆ 1           ┆ 2       ┆ 3     ┆ 0.666667     │
│ amount              ┆ values    ┆ 1           ┆ 1       ┆ 2     ┆ 0.5          │
│ date                ┆ values    ┆ 2           ┆ 0       ┆ 2     ┆ 0.0          │
│ first_name          ┆ values    ┆ 1           ┆ 1       ┆ 2     ┆ 0.5          │
│ flag                ┆ values    ┆ 1           ┆ 1       ┆ 2     ┆ 0.5          │
└─────────────────────┴───────────┴─────────────┴─────────┴───────┴──────────────┘
```

### Export to Excel
Export the results to an excel file.
```python
ds.to_excel()
```
