import os 
import pandas as pd

in_path = os.path.join('raw',
                       'short_term_repo',
                       'short-term-repo-omos-by-operation.XLSX')

rates_path = os.path.join('processed','general','full_daily_bank_rate.csv')

out_path = os.path.join('processed','short_term_repo','monthly_totals.csv')

df = pd.read_excel(in_path,
              header=1,
              usecols=[0,2,3,4],
              names=['op_date','mat_date','allocated','spread'])


df['op_date'] = pd.to_datetime(df['op_date'],format='%d/%m/%Y')
df['mat_date'] = pd.to_datetime(df['mat_date'],format='%d/%m/%Y')


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

repo_daily_ops = pd.concat(daily_rows, ignore_index=True)


daily_rates = pd.read_csv(rates_path,
                    usecols=[0,1],
                    names=['date','bank_rate'],
                    header=0)

daily_rates['date'] = pd.to_datetime(daily_rates['date'])


df = repo_daily_ops.merge(daily_rates, on='date', how='left')


df['interest_total'] = (df['allocated'] * (df['bank_rate'] + df['spread'])) / 365

daily_total = df.groupby('date', as_index=False)['interest_total'].sum()

monthly = (
    daily_total
        .set_index('date')
        .resample('ME')['interest_total']
        .sum()
        .round(3)            
)

monthly.to_csv(out_path)