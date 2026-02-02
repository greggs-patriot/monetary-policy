import os
import pandas as pd



gilt_interest = pd.read_csv(os.path.join('raw','interest_on_debt.csv'),
                            skiprows=408,
                            names=['date','gilts'])

gilt_interest['date'] = gilt_interest['date'].astype(str).str.strip()
gilt_interest['date'] = (pd.to_datetime(gilt_interest['date'],format='%Y %b')
                        + pd.offsets.MonthEnd(0))

print(gilt_interest)

interest_on_reserves = pd.read_csv(os.path.join('raw','smf_liabilites.csv'),
                            usecols=[0,3],
                            names=['date','reserves'],
                            header=0)

rates = pd.read_csv(os.path.join('raw','rates_and_ranges.csv'),
                    usecols=[0,1],
                    names=['date','bank_rate'],
                    header=0)


df = rates.merge(interest_on_reserves,on='date')
df['date'] = pd.to_datetime(df['date'], format='%d %b %y')

df = df.set_index('date')

# Create full daily index
full_index = pd.date_range(df.index.min(),
                           df.index.max(),
                           freq='D')

# Expand and forward-fill
df_daily = df.reindex(full_index).ffill()
df_daily.index.name = 'date'

df_daily['interest_paid'] = ((df_daily['bank_rate'] / 100) * df_daily['reserves']) / 365

#group by monthly
monthly = pd.DataFrame(df_daily['interest_paid'].resample('ME').sum())


monthly = monthly.merge(gilt_interest,left_index=True,right_on='date')
monthly = monthly.set_index('date')

print(monthly)