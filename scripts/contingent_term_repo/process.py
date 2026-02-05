import os 
import pandas as pd

path = os.path.join('raw',
                    'contingent_term_repo',
                    'contingent-term-repo-operations-results.xlsx')

rates_path = os.path.join('processed','general','full_daily_bank_rate.csv')

out_path = os.path.join('processed','contingent_term_repo','monthly_totals.csv')

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

daily_op_df = pd.concat(daily_rows, ignore_index=True)

daily_rates = pd.read_csv(rates_path,
                    usecols=[0,1],
                    names=['date','bank_rate'],
                    header=0)

daily_rates['date'] = pd.to_datetime(daily_rates['date'])


daily_op_df = daily_op_df.merge(daily_rates, on='date', how='left')


daily_op_df['interest_total'] = (
    (daily_op_df['allocated'] * (daily_op_df['bank_rate'] + daily_op_df['spread'])) / 365
)

daily_total : pd.DataFrame = daily_op_df.groupby('date', as_index=False)['interest_total'].sum()

monthly  = (
        daily_total
        .set_index('date')
        .resample('ME')['interest_total']
        .sum()       
        .round(3) 
            
)

monthly = monthly.loc[monthly > 0]
monthly.to_csv(out_path)