import os
import pandas as pd



gilt_interest = pd.read_csv(os.path.join('raw','interest_on_debt.csv'),
                            skiprows=408,
                            names=['date','gilts'])

interest_on_reserves = pd.read_csv(os.path.join('raw','smf_liabilites.csv'),
                            usecols=[0,3],
                            names=['date','reserves'],
                            header=0)

rates = pd.read_csv(os.path.join('raw','rates_and_ranges.csv'),
                    usecols=[0,1],
                    names=['date','bank_rate'],
                    header=0)


#print(rates['date'].dtype)
#print(interest_on_reserves['date'].dtype)
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
monthly = df_daily['interest_paid'].resample('ME').sum()

print(monthly)