import os 
import pandas as pd

path = os.path.join('raw',
                    'contingent_term_repo',
                    'contingent-term-repo-operations-results.xlsx')

rates_path = os.path.join('raw','general','daily_bank_rate.csv')

df = pd.read_excel(path,
                    header=1,
                    usecols=[0,2,4,5],
                    names=['op_date','mat_date','allocated','spread'])

df['op_date'] = pd.to_datetime(df['op_date'])
df['mat_date'] = pd.to_datetime(df['mat_date'])

daily_rows = []
for _, row in df.iterrows():
    dates = pd.date_range(row['op_date'], row['mat_date'] - pd.Timedelta(days=1))

    daily_rows.append(
        pd.DataFrame(
            {
                'date': dates,
                'allocated': row['allocated'],               
                'spread': row['spread'] / 10_000,
            }
        )
    )

daily_df = pd.concat(daily_rows, ignore_index=True)

daily_rates = pd.read_csv(rates_path,
                    usecols=[0,1],
                    names=['date','bank_rate'],
                    header=0)

daily_rates['date'] = pd.to_datetime(daily_rates['date'],format='%d %b %y')
daily_rates['bank_rate'] = daily_rates['bank_rate'] / 100

daily_df = daily_df.merge(daily_rates, on='date', how='left')


daily_df['interest_total'] = (
    (daily_df['allocated'] * (daily_df['bank_rate'] + daily_df['spread'])) / 365
)


daily_df.to_csv('testing.csv')