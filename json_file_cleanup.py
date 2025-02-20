# Databricks notebook source
import pandas as pd
import json
import matplotlib.pyplot as plt

# COMMAND ----------

current_year = 2024

file_name = f'/Volumes/tabular/dataexpert/jw_capstone/{current_year}_all_advgames.json'

# Load the raw JSON data from the file
with open(file_name, 'r') as file:
    data = json.load(file)

# Convert the JSON data into a DataFrame
df = pd.DataFrame(data)

column_names = [
    "numdate", "datetext", "opstyle", "quality", "win1", "opponent", "muid", "win2",
    "Min_per", "ORtg", "Usage", "eFG", "TS_per", "ORB_per", "DRB_per", "AST_per", "TO_per",
    "dunksmade", "dunksatt", "rimmade", "rimatt", "midmade", "midatt", "twoPM", "twoPA", 
    "TPM", "TPA", "FTM", "FTA", "bpm_rd", "Obpm", "Dbpm", "bpm_net", "pts", "ORB", "DRB", 
    "AST", "TOV", "STL", "BLK", "stl_per", "blk_per", "PF", "possessions", "bpm", "sbpm", 
    "loc", "tt", "pp", "inches", "cls", "pid", "year"
]

df.columns = column_names

df.head()

# COMMAND ----------

# Display the first few rows of the DataFrame
print(df.head())
print(df.shape)


# COMMAND ----------

# Check for columns with NaNs and Nones:
columns_with_nans = df.columns[df.isna().any()].tolist()
print(f"columns with NaNs: {columns_with_nans}")

# COMMAND ----------

# Check which numeric columns have negative values
numeric_columns = df.select_dtypes(include='number').columns
columns_with_negatives = numeric_columns[(df[numeric_columns] < 0).any()].tolist()
known_negatives = ['bpm_rd', 'Obpm', 'Dbpm', 'bpm_net', 'bpm', 'sbpm']
columns_with_negatives = [col for col in columns_with_negatives if col not in known_negatives]
print(f"columns with surprise negative values: {columns_with_negatives}")

# COMMAND ----------

# Clean up issues found in 2018 dataset

# Fill NaN and None values
df['cls'] = df['cls'].fillna('unknown')
df['inches'] = df['inches'].fillna(0)

# Fill negative values with 0
columns_to_fill = ['Usage', 'AST_per', 'TO_per', 'twoPM', 'twoPA']
for col in columns_to_fill:
    df.loc[df[col] < 0, col] = 0

# Drop unwanted columns
columns_to_drop = ['opstyle', 'quality', 'loc', 'datetext', 'muid']
df = df.drop(columns=columns_to_drop)

# COMMAND ----------

# 2018 players with multiple pids
# df[df['pp'] == 'Zach Smith']
# df[df['pp'] == 'Jordan Jones']

# Group by player name and count unique player IDs and teams
grouped = df.groupby('pp').agg(
    unique_player_ids=('pid', 'nunique'),
    unique_teams=('tt', 'nunique')
).reset_index()

# Filter to show only player names with multiple unique player IDs
players_with_multiple_ids = grouped[grouped['unique_player_ids'] > 1]

# Confirm that the number of unique player IDs matches the number of unique teams
result = players_with_multiple_ids[players_with_multiple_ids['unique_player_ids'] == players_with_multiple_ids['unique_teams']]

# raw delta between number of players names and player ids in the overall dataset
delta = len(df['pid'].unique()) - len(df['pp'].unique())
# number of players with multiple pids
multi_pid_players = len(result)
# bonus pids from players with more than 2 pids
bonus_pids = ((result[result['unique_player_ids'] > 2]['unique_player_ids']) - 2).sum()

# First check that players with multiple pids have the same number of unique player IDs as the number of unique teams
check1 = (result['unique_player_ids'] != result['unique_teams']).sum() == 0
# Then check that the overall delta is equal to the number of players with more than 2 pids + any bonus pids
check2 = delta - len(result) - bonus_pids == 0

print(f"Number of unique player names: {len(df['pp'].unique())}")
print(f"Number of unique player ids: {len(df['pid'].unique())}")
print(f"Delta: {delta}")
print(f"Number of players with multiple pids: {len(result)}")
(result['unique_player_ids'] != result['unique_teams']).sum()
print(f"Total 'bonus' pids from players with more than 2 pids: {bonus_pids}")

if check1 and check2:
    print("DQC passed!  Delta b/t number of players names and player ids is acccounted for.")
else:
    print("ERROR: Delta between delta and number of players with multiple pids is NOT acccounted for!")

result



# COMMAND ----------

offset = 1
column_num = 20 - offset
print(df.columns[column_num])
print(str(len(df.iloc[:, column_num].unique())) + " unique values (sorted):")
sorted(df.iloc[:, column_num].unique().tolist())

# COMMAND ----------

# Plot historgram of dataframe column
df['Usage'].hist(bins=30)

# Display the histogram
plt.show()


# COMMAND ----------

for i in range(0,1):
    year = str(current_year + i)
    path = f"/Volumes/tabular/dataexpert/jw_capstone/clean_csv/{year}_all_advgames_cleaned.csv"
    print(path)
    df.to_csv(path)

# COMMAND ----------

rows = 0
for i in range(0,10):
    year = str(2015 + i)
    file_path = f"/Volumes/tabular/dataexpert/jw_capstone/clean_csv/{year}_all_advgames_cleaned.csv"
    print(file_path)
    df = pd.read_csv(file_path)
    print(len(df))
    rows = rows + len(df)
print(f"Total rows: {rows}")

# COMMAND ----------

