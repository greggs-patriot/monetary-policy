import os
import pandas as pd

in_path = os.path.join('raw','general','asset_purchase_facility.csv')
rates_path = os.path.join('processed','general','full_daily_bank_rate.csv')
out_path = os.path.join('processed','general','cost_of_mp.csv')

df = pd.read_csv(in_path,header=0,names=['date','total_purchased'])
rates = pd.read_csv(rates_path)

df['date'] = pd.to_datetime(df['date'],format='%d %b %y')
df = df.set_index('date')

rates['date'] = pd.to_datetime(rates['date'])

date_range = pd.date_range(df.index.min(),
                            df.index.max(),
                            freq='D')

# Expand and forward-fill
df = df.reindex(date_range).ffill()   
df.index.name = 'date'

df = df.merge(rates,how='left',on='date')
df['interest'] = ((df['total_purchased'] * df['bank_rate']) / 365)
df = df.set_index('date')

df = df['interest'].resample('ME').sum().round(3)

df.to_csv(out_path)