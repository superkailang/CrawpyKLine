import pyarrow.parquet as pq

import numpy as np

import pandas as pd

import pyarrow as pa
import datasets

dataset = datasets.load_dataset("custom.py")

print(dataset)


import pandas as pd
result = pd.read_parquet('train-00000-of-00001-560c9916b64505b8.parquet', engine='pyarrow')


df = pd.DataFrame({'one': [-1, np.nan, 2.5],
                   'two': ['foo', 'bar', 'baz'],
                   'three': [True, False, True]},
                   index=list('abc'))

table = pa.Table.from_pandas(df)
pq.write_table(table, 'example.parquet')