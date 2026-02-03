import os
import pandas as pd



gilt_interest = pd.read_csv(os.path.join('raw','interest_on_debt.csv'),
                            skiprows=408,
                            names=['date','gilts'])

gilt_interest['date'] = gilt_interest['date'].astype(str).str.strip()
gilt_interest['date'] = (pd.to_datetime(gilt_interest['date'],format='%Y %b')
                        + pd.offsets.MonthEnd(0))

#print(gilt_interest)

liabilites = pd.read_csv(os.path.join('raw','smf_liabilites.csv'),
                            usecols=[0,1,3,4],
                            names=['date','deposit','reserves','op_deposit'],
                            header=0)
assets = pd.read_csv(os.path.join('raw','smf_assets.csv'),
                            usecols=[0,4,7,8],
                            names=['date','lending','op_lending','short_term_repo'],
                            header=0)

rates = pd.read_csv(os.path.join('raw','rates_and_ranges.csv'),
                    usecols=[0,1,2,3,4,5],
                    names=['date','bank_r','lending_r','deposit_r','op_lending_r','op_deposit_r'],
                    header=0)

rates = rates.set_index('date')
liabilites = liabilites.set_index('date')
assets = assets.set_index('date')

def mat_to_month(rates : pd.DataFrame,amounts : pd.DataFrame):
    df : pd.DataFrame = rates.merge(amounts,on='date').fillna(0)
    df.index = pd.to_datetime(df.index, format='%d %b %y')
    
    
    # Create full daily index
    full_index = pd.date_range(df.index.min(),
                            df.index.max(),
                            freq='D')

    # Expand and forward-fill
    df_daily = df.reindex(full_index).ffill()
    df_daily.index.name = 'date'

    # get column names automatically
    rate_col = rates.columns[0]
    amount_col = amounts.columns[0]
    interest_col = 'interest_' + amount_col

    df_daily[interest_col] = ((df_daily[rate_col] / 100) * df_daily[amount_col]) / 365

    #group by monthly
    return pd.DataFrame(df_daily[interest_col].resample('ME').sum()).round(3)

    


#monthly = monthly.merge(gilt_interest,left_index=True,right_on='date')
#monthly = monthly.set_index('date')

dfs = [mat_to_month(rates[['bank_r']],liabilites[['reserves']]),
                mat_to_month(rates[['deposit_r']],liabilites[['deposit']]),
                mat_to_month(rates[['op_deposit_r']],liabilites[['op_deposit']]),
                mat_to_month(rates[['bank_r']],assets[['short_term_repo']]),
                mat_to_month(rates[['op_lending_r']],assets[['op_lending']]),
                mat_to_month(rates[['lending_r']],assets[['lending']])]
 #               left_index=True,
  #              right_index=True)

pd.concat(dfs, axis=1).to_csv('test.csv')