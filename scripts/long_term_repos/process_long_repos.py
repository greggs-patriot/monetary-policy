import os
import pandas as pd

PATH = os.path.join('raw','long_term_repos','long-term-repo-omos-by-operation.xls')
OUT_PATH = os.path.join('processed','long_term_repos','long_term_repos_1.csv')

df = pd.read_excel(PATH,
                   sheet_name='LTR Summary',
                   usecols=[0,3,5,8],
                   names=['op_date','mat_date','amount','rate']).dropna(how="all")


df["op_date"] = pd.to_datetime(df["op_date"], dayfirst=True)
df["mat_date"] = pd.to_datetime(df["mat_date"], dayfirst=True)

df["op_id"] = df.index

# Daily interest per operation (£m)
df["daily_interest"] = df["amount"] * df["rate"] / 100 / 365


rows = []

for _, r in df.iterrows():
    dates = pd.date_range(r.op_date, r.mat_date - pd.Timedelta(days=1), freq="D")
    
    tmp = pd.DataFrame({
        "date": dates,
        "op_id": r.op_id,
        "interest": r.daily_interest
    })
    
    rows.append(tmp)

daily = pd.concat(rows)

# Sum across operations
daily_total = daily.groupby("date", as_index=False)["interest"].sum()

monthly = (
    daily_total
        .set_index("date")
        .resample("ME")["interest"]
        .sum()
       # .round(3) 
            
)

monthly.to_csv(OUT_PATH)