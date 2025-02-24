# Databricks notebook source
# %python
# %pip install databricks-dlt
# %restart_python

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, BooleanType, DoubleType, TimestampType, ArrayType

# Define the schema for PlayerGames
# Define the schema for PlayerGames
player_game_schema = StructType([
    StructField("StatID", IntegerType(), True),
    StructField("TeamID", IntegerType(), True),
    StructField("PlayerID", IntegerType(), True),
    StructField("SeasonType", IntegerType(), True),
    StructField("Season", IntegerType(), True),
    StructField("Name", StringType(), True),
    StructField("Team", StringType(), True),
    StructField("Position", StringType(), True),
    StructField("FanDuelSalary", IntegerType(), True),
    StructField("DraftKingsSalary", IntegerType(), True),
    StructField("FantasyDataSalary", IntegerType(), True),
    StructField("YahooSalary", IntegerType(), True),
    StructField("InjuryStatus", StringType(), True),
    StructField("InjuryBodyPart", StringType(), True),
    StructField("InjuryStartDate", TimestampType(), True),
    StructField("InjuryNotes", StringType(), True),
    StructField("FanDuelPosition", StringType(), True),
    StructField("DraftKingsPosition", StringType(), True),
    StructField("YahooPosition", StringType(), True),
    StructField("OpponentRank", IntegerType(), True),
    StructField("OpponentPositionRank", IntegerType(), True),
    StructField("GlobalTeamID", IntegerType(), True),
    StructField("GameID", IntegerType(), True),
    StructField("OpponentID", IntegerType(), True),
    StructField("Opponent", StringType(), True),
    StructField("Day", TimestampType(), True),
    StructField("DateTime", TimestampType(), True),
    StructField("HomeOrAway", StringType(), True),
    StructField("IsGameOver", BooleanType(), True),
    StructField("GlobalGameID", IntegerType(), True),
    StructField("GlobalOpponentID", IntegerType(), True),
    StructField("Updated", TimestampType(), True),
    StructField("Games", IntegerType(), True),
    StructField("FantasyPoints", DoubleType(), True),
    StructField("Minutes", IntegerType(), True),
    StructField("FieldGoalsMade", IntegerType(), True),
    StructField("FieldGoalsAttempted", IntegerType(), True),
    StructField("FieldGoalsPercentage", DoubleType(), True),
    StructField("EffectiveFieldGoalsPercentage", DoubleType(), True),
    StructField("TwoPointersMade", IntegerType(), True),
    StructField("TwoPointersAttempted", IntegerType(), True),
    StructField("TwoPointersPercentage", DoubleType(), True),
    StructField("ThreePointersMade", IntegerType(), True),
    StructField("ThreePointersAttempted", IntegerType(), True),
    StructField("ThreePointersPercentage", DoubleType(), True),
    StructField("FreeThrowsMade", IntegerType(), True),
    StructField("FreeThrowsAttempted", IntegerType(), True),
    StructField("FreeThrowsPercentage", DoubleType(), True),
    StructField("OffensiveRebounds", IntegerType(), True),
    StructField("DefensiveRebounds", IntegerType(), True),
    StructField("Rebounds", IntegerType(), True),
    StructField("OffensiveReboundsPercentage", DoubleType(), True),
    StructField("DefensiveReboundsPercentage", DoubleType(), True),
    StructField("TotalReboundsPercentage", DoubleType(), True),
    StructField("Assists", IntegerType(), True),
    StructField("Steals", IntegerType(), True),
    StructField("BlockedShots", IntegerType(), True),
    StructField("Turnovers", IntegerType(), True),
    StructField("PersonalFouls", IntegerType(), True),
    StructField("Points", IntegerType(), True),
    StructField("TrueShootingAttempts", DoubleType(), True),
    StructField("TrueShootingPercentage", DoubleType(), True),
    StructField("PlayerEfficiencyRating", DoubleType(), True),
    StructField("AssistsPercentage", DoubleType(), True),
    StructField("StealsPercentage", DoubleType(), True),
    StructField("BlocksPercentage", DoubleType(), True),
    StructField("TurnOversPercentage", DoubleType(), True),
    StructField("UsageRatePercentage", DoubleType(), True),
    StructField("FantasyPointsFanDuel", DoubleType(), True),
    StructField("FantasyPointsDraftKings", DoubleType(), True),
    StructField("FantasyPointsYahoo", DoubleType(), True)
])

# COMMAND ----------

# Define the schema mapping
schema_mapping = {
    "StatID": int,
    "TeamID": int,
    "PlayerID": int,
    "SeasonType": int,
    "Season": int,
    "Name": str,
    "Team": str,
    "Position": str,
    "FanDuelSalary": int,
    "DraftKingsSalary": int,
    "FantasyDataSalary": int,
    "YahooSalary": int,
    "InjuryStatus": str,
    "InjuryBodyPart": str,
    "InjuryStartDate": 'datetime64[ns]',
    "InjuryNotes": str,
    "FanDuelPosition": str,
    "DraftKingsPosition": str,
    "YahooPosition": str,
    "OpponentRank": int,
    "OpponentPositionRank": int,
    "GlobalTeamID": int,
    "GameID": int,
    "OpponentID": int,
    "Opponent": str,
    "Day": 'datetime64[ns]',
    "DateTime": 'datetime64[ns]',
    "HomeOrAway": str,
    "IsGameOver": bool,
    "GlobalGameID": int,
    "GlobalOpponentID": int,
    "Updated": 'datetime64[ns]',
    "Games": int,
    "FantasyPoints": float,
    "Minutes": int,
    "FieldGoalsMade": int,
    "FieldGoalsAttempted": int,
    "FieldGoalsPercentage": float,
    "EffectiveFieldGoalsPercentage": float,
    "TwoPointersMade": int,
    "TwoPointersAttempted": int,
    "TwoPointersPercentage": float,
    "ThreePointersMade": int,
    "ThreePointersAttempted": int,
    "ThreePointersPercentage": float,
    "FreeThrowsMade": int,
    "FreeThrowsAttempted": int,
    "FreeThrowsPercentage": float,
    "OffensiveRebounds": int,
    "DefensiveRebounds": int,
    "Rebounds": int,
    "OffensiveReboundsPercentage": float,
    "DefensiveReboundsPercentage": float,
    "TotalReboundsPercentage": float,
    "Assists": int,
    "Steals": int,
    "BlockedShots": int,
    "Turnovers": int,
    "PersonalFouls": int,
    "Points": int,
    "TrueShootingAttempts": float,
    "TrueShootingPercentage": float,
    "PlayerEfficiencyRating": float,
    "AssistsPercentage": float,
    "StealsPercentage": float,
    "BlocksPercentage": float,
    "TurnOversPercentage": float,
    "UsageRatePercentage": float,
    "FantasyPointsFanDuel": float,
    "FantasyPointsDraftKings": float,
    "FantasyPointsYahoo": float
}

# COMMAND ----------

# import requests
# import pandas as pd
# from pyspark.sql.functions import col, explode, from_json, to_json

# # Fetch data from the API
# def fetch_data_from_api(api_url):
#     response = requests.get(api_url)
#     response.raise_for_status()  # Raise an error for bad status codes
#     return response.json()

# api_url = "https://replay.sportsdata.io/api/v3/cbb/stats/json/boxscoresdelta/2023-12-02/all?key=bafecc01eaaf419a984cd7ec2b602594"
# json_data = fetch_data_from_api(api_url)

# COMMAND ----------

# pdf = pd.json_normalize(json_data, sep='_')
# pdf = pd.DataFrame(json_data)
# pdf.head(3)

# COMMAND ----------

# list(pdf.columns)

# COMMAND ----------

# # Normalize the 'Game' column
# df_games = pd.json_normalize(pdf['Game'])

# # Check if 'Stadium' and 'Periods' columns exist before dropping them
# columns_to_drop = ['Stadium', 'Periods']
# existing_columns_to_drop = [col for col in columns_to_drop if col in df_games.columns]
# df_games = df_games.drop(columns=existing_columns_to_drop)

# # Display the resulting DataFrame
# display(df_games)

# COMMAND ----------

# # Remove 'Stadium' and 'Periods' from each dictionary in the 'Game' column
# pdf['Game'] = pdf['Game'].apply(lambda x: {k: v for k, v in x.items() if k not in ['Stadium', 'Periods']})

# # Create a new DataFrame from the modified 'Game' column
# df_games = pd.json_normalize(pdf['Game'])

# # Display the resulting DataFrame
# display(df_games)

# COMMAND ----------

# pdf_exploded = pdf.explode('PlayerGames')
# df_player_games = pd.json_normalize(pdf_exploded['PlayerGames'])

# COMMAND ----------

# pdf_exploded = pdf.explode('TeamGames')
# df_team_games = pd.json_normalize(pdf_exploded['TeamGames'])

# COMMAND ----------

# pdf_exploded = pdf.explode('Periods')
# df_periods = pd.json_normalize(pdf_exploded['Periods'])

# COMMAND ----------

import requests
import pandas as pd
from pyspark.sql.functions import col, explode, from_json, to_json

# Fetch data from the API
def fetch_data_from_api(api_url):
    response = requests.get(api_url)
    response.raise_for_status()  # Raise an error for bad status codes
    return response.json()

# Fill NaN values: 0 for numeric columns, blank string for string columns
def fill_nas(df):
    for column in df.columns:
        if pd.api.types.is_numeric_dtype(df[column]):
            df[column] = df[column].fillna(0)
        elif pd.api.types.is_string_dtype(df[column]):
            df[column] = df[column].fillna('')
    return df

api_url = "https://replay.sportsdata.io/api/v3/cbb/stats/json/boxscoresdelta/2023-12-02/all?key=bafecc01eaaf419a984cd7ec2b602594"
json_data = fetch_data_from_api(api_url)

# Convert the JSON data to a pandas DataFrame
# pdf = pd.json_normalize(json_data, sep='_')
pdf = pd.DataFrame(json_data)

pdf_exploded = pdf.explode('PlayerGames')
df_player_games = pd.json_normalize(pdf_exploded['PlayerGames'])

# Cast each column to the appropriate type
# for column, dtype in schema_mapping.items():
#     if column in df_player_games.columns:
#         df_player_games[column] = df_player_games[column].astype(dtype)


df_player_games = fill_nas(df_player_games)
# df_games = fill_nas(df_games)

df_player_games = df_player_games.astype(schema_mapping)

# Normalize the 'Game' column
# df_games = pd.json_normalize(pdf['Game'])

# # Check if 'Stadium' and 'Periods' columns exist before dropping them
# columns_to_drop = ['Stadium', 'Periods']
# existing_columns_to_drop = [col for col in columns_to_drop if col in df_games.columns]
# df_games = df_games.drop(columns=existing_columns_to_drop)



df_player_games = spark.createDataFrame(df_player_games)

# Define the Delta Live Table
@dlt.table(
    name="jw_raw_player_games",
    comment="Table containing player games data"
)
def load_player_games_data():
    return df_player_games

# COMMAND ----------

# @dlt.table(
#     name="jw_raw_stadiums",
#     comment="Table containing stadium data"
# )
# def load_stadium_data():
#     return stadium_df

# @dlt.table(
#     name="jw_raw_games",
#     comment="Table containing game data"
# )
# def load_game_data():
#     return game_df

# @dlt.table(
#     name="jw_raw_periods",
#     comment="Table containing periods data"
# )
# def load_periods_data():
#     return periods_df

# @dlt.table(
#     name="jw_raw_player_games",
#     comment="Table containing player games data"
# )
# def load_player_games_data():
#     return player_games_df

# @dlt.table(
#     name="jw_raw_team_games",
#     comment="Table containing team games data"
# )
# def load_team_games_data():
#     return team_games_df

# COMMAND ----------

