import glob
import os

import pandas as pd

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

training_data = pd.concat(dfs, ignore_index=True)

# what is done, added all the csv files in pandas dataframe; now ready to train model

# target(what we are predicting) is capacity, using all the columns as features in the training data; axis=1 means drop the column
target = training_data["maximumEnrollment"]
training_data = training_data.drop("maximumEnrollment", axis=1)
