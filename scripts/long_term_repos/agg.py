import os
import pandas as pd

out_paths = [os.path.join('processed','long_term_repos',f) 
         for f in ['total_monthly.csv','total_yearly.csv']]

paths = [os.path.join('processed','long_term_repos',f) 
         for f in ['long_term_repos_1.csv','long_term_repos_2.csv','long_term_repos_3.csv']]

dfs = [pd.read_csv(p) for p in paths]

gen_path = os.path.join('processed','general','test.csv')
gen_df = pd.read_csv(gen_path,usecols=[0,11],names=['date','total_interest_est'],header=1)


df = (pd.concat(dfs).groupby('date')
    .sum()
    .merge(gen_df,how='outer',on='date'))

(df.round(3)
   .to_csv(out_paths[0],index=False))

df['date'] = pd.to_datetime(df['date'])
df_filtered = df[
    (df['date'].dt.year >= 2007) &
    (df['date'].dt.year <= 2024)
]

yearly : pd.DataFrame = (
    df_filtered
    .groupby(df_filtered['date'].dt.year)[
        ['interest_total', 'total_interest_est']
    ]
    .sum()   
)

yearly.loc['Total'] = yearly.sum()
yearly.round(3).to_csv(out_paths[1])
