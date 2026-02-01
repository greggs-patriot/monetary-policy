import os
import pandas as pd



debt = pd.read_csv(os.path.join('raw','interest_on_debt.csv'),skiprows=408,names=['date','interest_paid'])
print(debt)