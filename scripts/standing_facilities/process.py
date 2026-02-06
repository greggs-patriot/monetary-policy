import os
import pandas as pd

in_path = os.path.join('processed','general','test.csv')
out_path = os.path.join('processed','standing_facilities','monthly_totals.csv')



df = pd.read_csv(in_path,
                  usecols=[0,2,3,5,6],
                  names=['date','df','odf','olf','lf'],
                  header=0)

df['deposit_facility'] = df['df'] + df['odf']
df['lending_facility'] = df['lf'] + df['olf']


df['net'] = df['lending_facility'] - df['deposit_facility']

(
    df.loc[
        (df['deposit_facility'] > 0) | (df['lending_facility'] > 0),
        ['date', 'deposit_facility', 'lending_facility', 'net']
    ]
    .round(3)
    .to_csv(out_path, index=False)
)



