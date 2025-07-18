import pandas as pd
import glob
import os


dfs = []
csv_data = glob.glob("csv_data/*.csv")

# go through each file
for csv_file in csv_data: 
  # grab each filename and check for dates
  fileName = os.path.basename(csv_file)
  for year in range(2018,2024):
    if str(year) in fileName:
      df = pd.read_csv(csv_file)
      df["year"] = year # tagging year to know where each csv file is from
      dfs.append(df)

final_df = pd.concat(dfs, ignore_index=True)

