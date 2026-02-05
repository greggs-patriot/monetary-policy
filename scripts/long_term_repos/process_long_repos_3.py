import os
import pandas as pd

RAW_DIR = 'raw'
OUTPUT_DIR = 'processed'

repo_file = 'indexed-long-term-repo-omos-by-operation.XLSX'   # 2014–present file
rates_file = 'daily_bank_rate.csv'

repo_path = os.path.join(RAW_DIR,'long_term_repos',repo_file)
rates_path = os.path.join(RAW_DIR,'general',rates_file)

output_path = os.path.join(OUTPUT_DIR,'long_term_repos','long_term_repos_3.csv')

# -------------------------------
# Load repo ops (only needed cols)
# -------------------------------

repos = pd.read_excel(
    repo_path,
    skiprows=2,
    # op date, maturity, alloc A/B/C, spread A/B/C
    usecols=[0, 2, 7, 8, 9, 10, 11, 12],
    names=[
        'operation_date',
        'maturity_date',
        'alloc_A',
        'alloc_B',
        'alloc_C',
        'spread_A_bp',
        'spread_B_bp',
        'spread_C_bp',
    ],
    na_values=['-']
)

# Drop footer / notes: only keep rows with both dates present
repos = repos[
    repos['operation_date'].notna()
    & repos['maturity_date'].notna()
].fillna(0)

repos['operation_date'] = pd.to_datetime(repos['operation_date'])
repos['maturity_date'] = pd.to_datetime(repos['maturity_date'])

# -------------------------------
# Expand repos to daily (per-op rows)
# -------------------------------

daily_rows = []
for _, row in repos.iterrows():
    dates = pd.date_range(
        row['operation_date'],
        row['maturity_date'] - pd.Timedelta(days=1),
        freq='D',
    )

    daily_rows.append(
        pd.DataFrame(
            {
                'date': dates,
                'alloc_A': row['alloc_A'],
                'alloc_B': row['alloc_B'],
                'alloc_C': row['alloc_C'],
                'spread_A': row['spread_A_bp'] / 10_000,
                'spread_B': row['spread_B_bp'] / 10_000,
                'spread_C': row['spread_C_bp'] / 10_000,
            }
        )
    )

repo_daily_ops = pd.concat(daily_rows, ignore_index=True)

# -------------------------------
# Load daily Bank Rate
# -------------------------------

daily_rates = pd.read_csv(
    rates_path,
    usecols=[0, 1],
    names=['date', 'bank_rate'],
    header=0
)

daily_rates['date'] = pd.to_datetime(daily_rates['date'], format='%d %b %y')
daily_rates['bank_rate'] = daily_rates['bank_rate'] / 100


# (Optional but usually wise) ensure sorted then forward fill
daily_rates = daily_rates.sort_values('date')
daily_rates['bank_rate'] = daily_rates['bank_rate'].ffill()

# -------------------------------
# Merge & compute interest per op-day
# -------------------------------

df = repo_daily_ops.merge(daily_rates, on='date', how='inner')

df['interest_total'] = (
    (df['alloc_A'] * (df['bank_rate']  + df['spread_A'])) / 365
    + (df['alloc_B'] * (df['bank_rate'] + df['spread_B'])) / 365
    + (df['alloc_C'] * (df['bank_rate'] + df['spread_C'])) / 365
)

# -------------------------------
# Sum to daily total, then monthly total (month-end buckets)
# -------------------------------

daily_total = df.groupby('date', as_index=False)['interest_total'].sum()

monthly = (
    daily_total
        .set_index('date')
        .resample('ME')['interest_total']
        .sum()
    #    .round(3)
)

# -------------------------------
# Save monthly output
# -------------------------------

monthly.to_csv(output_path, index=True)
#print(monthly.head())
