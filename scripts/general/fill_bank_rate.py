import os
import pandas as pd

in_path = os.path.join('raw','general','daily_bank_rate.csv')
out_path = os.path.join('processed','general','full_daily_bank_rate.csv')

df = pd.read_csv(in_path,names=['date','bank_rate'],
                 header=0,
                 index_col='date')
                 

df.index = pd.to_datetime(df.index, format='%d %b %y')
df['bank_rate'] = df['bank_rate'] / 100

full_index = pd.date_range(df.index.min(),
                            df.index.max(),
                            freq='D')


# Expand and forward-fill
df = df.reindex(full_index).ffill()
df.index.name = 'date'

df.to_csv(out_path)