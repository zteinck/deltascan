from deltascan import DeltaScan
import pandas as pd
import polars as pl
import datetime


if __name__ == '__main__':

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

    # Perform comparison
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

    # Print summary
    print(ds.summary)

    # Export to Excel
    ds.to_excel()