# Databricks notebook source
from pyspark.sql.functions import col, lag, sum as _sum, when, coalesce, first, max
from pyspark.sql.window import Window

player_games = spark.read.table("tabular.dataexpert.jw_raw_player_games")
games = spark.read.table("tabular.dataexpert.jw_raw_games")

# Filter to columns of interest for game
columns_to_keep = ["GameID", "Status", "TimeRemainingMinutes", "AwayTeamID", "HomeTeamID", "AwayTeamScore", "HomeTeamScore", "timestamp"]
games = games.select([col(column) for column in columns_to_keep])
# games = games.filter(games["GameID"] == 58925)
# games = games.filter(games["Status"] == 'InProgress')

# Filter to columns of interest for player_game
columns_to_keep = ["GameID", "TeamID", "PlayerID", "Minutes", "timestamp"]
player_games = player_games.select([col(column) for column in columns_to_keep])
# player_games = player_games.filter(player_games["GameID"] == 58925)

# player_games.display()

# Calculate the change in scores
games_window_spec = Window.partitionBy("GameID").orderBy("timestamp")
games = games.withColumn("PrevHomeTeamScore", lag("HomeTeamScore").over(games_window_spec))
games = games.withColumn("PrevAwayTeamScore", lag("AwayTeamScore").over(games_window_spec))
games = games.withColumn("HomeTeamScoreChange", col("HomeTeamScore") - col("PrevHomeTeamScore"))
games = games.withColumn("AwayTeamScoreChange", col("AwayTeamScore") - col("PrevAwayTeamScore"))

# Calculate the change in minutes
player_games_window_spec = Window.partitionBy("GameID", "PlayerID").orderBy("timestamp")
player_games = player_games.withColumn("PrevMinutes", lag("Minutes").over(player_games_window_spec))
player_games = player_games.withColumn("MinutesChange", col("Minutes") - col("PrevMinutes"))

# filtered = player_games.filter(col("PlayerID") == 60024441)
# display(filtered)

# Eliminate rows with negative minute changes (probably a stat correction...)
player_games = player_games.filter(player_games["MinutesChange"] >= 0)

# Rename the timestamp column in one of the DataFrames to avoid ambiguity
# games = games.withColumnRenamed("timestamp", "game_timestamp")

# Join the tables to calculate the plus-minus metric
join_condition = [
    games["GameID"] == player_games["GameID"],
    games["timestamp"] == player_games["timestamp"]
    ]
# Perform the join
plus_minus = games.join(player_games, join_condition, "inner")

# if games.count() == plus_minus.count():
#     print(f"The join resulted in expected number of rows: {plus_minus.count()}.")
#     # TODO: lag function above creates an extra row with nulls in the games df
#     # This extra row is left behind by the inner join...
# else:
#     print(f"WARNING: Unexpected number of rows after join: {plus_minus.count()} instead of {games.count()}")

# Determine row plus_minus based on player's team
plus_minus = plus_minus.withColumn(
    "PlusMinusChange",
    when(
        col("TeamID") == col("HomeTeamID"),
        col("HomeTeamScoreChange") - col("AwayTeamScoreChange")
    ).otherwise(
        col("AwayTeamScoreChange") - col("HomeTeamScoreChange")
    )
)

# Only credit/debit a player if they were in the game
plus_minus = plus_minus.filter(plus_minus["MinutesChange"] > 0)

# filtered = player_games.filter(col("PlayerID") == 60028275)

# Group by PlayerID and GameID, and calculate the sum of PlusMinusChange
grouped_df = plus_minus.groupBy("PlayerID", games.GameID).agg(
    sum("PlusMinusChange").alias("TotalPlusMinusChange"), 
    first("TeamID").alias("TeamID"), 
    max("Minutes").alias("Total Minutes")
    )

# # Display the new DataFrame
# display(plus_minus)
display(grouped_df)
# display(filtered)

# COMMAND ----------

# from pyspark.sql.functions import col, lag, sum as _sum, when, coalesce, first, max
# from pyspark.sql.window import Window

# player_games = spark.read.table("tabular.dataexpert.jw_raw_player_games")
# games = spark.read.table("tabular.dataexpert.jw_raw_games")

# # Filter to columns of interest for game
# columns_to_keep = ["GameID", "Status", "TimeRemainingMinutes", "AwayTeamID", "HomeTeamID", "AwayTeamScore", "HomeTeamScore", "timestamp"]
# games = games.select([col(column) for column in columns_to_keep])
# games = games.filter(games["GameID"] == 54944)
# # games = games.filter(games["Status"] == 'InProgress')

# # Filter to columns of interest for player_game
# columns_to_keep = ["GameID", "TeamID", "PlayerID", "Minutes", "timestamp"]
# player_games = player_games.select([col(column) for column in columns_to_keep])
# player_games = player_games.filter(player_games["GameID"] == 54944)
# # player_games = player_games.filter(player_games["PlayerID"] == 60019902)

# # Calculate the change in scores
# window_spec = Window.partitionBy("GameID").orderBy("timestamp")
# games = games.withColumn("PrevHomeTeamScore", lag("HomeTeamScore").over(window_spec))
# games = games.withColumn("PrevAwayTeamScore", lag("AwayTeamScore").over(window_spec))
# games = games.withColumn("HomeTeamScoreChange", col("HomeTeamScore") - col("PrevHomeTeamScore"))
# games = games.withColumn("AwayTeamScoreChange", col("AwayTeamScore") - col("PrevAwayTeamScore"))

# # Calculate the change in minutes
# player_games = player_games.withColumn("PrevMinutes", lag("Minutes").over(window_spec))
# player_games = player_games.withColumn("MinutesChange", col("Minutes") - col("PrevMinutes"))

# # Eliminate rows with negative minute changes (probably a stat correction...)
# player_games = player_games.filter(player_games["MinutesChange"] >= 0)

# # Rename the timestamp column in one of the DataFrames to avoid ambiguity
# # games = games.withColumnRenamed("timestamp", "game_timestamp")

# # Join the tables to calculate the plus-minus metric
# join_condition = [
#     games["GameID"] == player_games["GameID"],
#     games["timestamp"] == player_games["timestamp"]
#     ]
# # Perform the join
# plus_minus = games.join(player_games, join_condition, "inner")

# if games.count() == plus_minus.count():
#     print(f"The join resulted in expected number of rows: {plus_minus.count()}.")
#     # TODO: lag function above creates an extra row with nulls in the games df
#     # This extra row is left behind by the inner join...
# else:
#     print(f"WARNING: Unexpected number of rows after join: {plus_minus.count()} instead of {games.count()}")

# # Determine row plus_minus based on player's team
# plus_minus = plus_minus.withColumn(
#     "PlusMinusChange",
#     when(
#         col("TeamID") == col("HomeTeamID"),
#         col("HomeTeamScoreChange") - col("AwayTeamScoreChange")
#     ).otherwise(
#         col("AwayTeamScoreChange") - col("HomeTeamScoreChange")
#     )
# )

# # Group by PlayerID and GameID, and calculate the sum of PlusMinusChange
# grouped_df = plus_minus.groupBy("PlayerID", games.GameID).agg(
#     sum("PlusMinusChange").alias("TotalPlusMinusChange"), 
#     first("TeamID").alias("TeamID"), 
#     max("Minutes").alias("Total Minutes")
#     )

# # # Display the new DataFrame
# display(grouped_df)


# COMMAND ----------

# from pyspark.sql.functions import col, sum

# # Assuming 'sub' is your DataFrame and 'PlusMinusChange' is the column you want to sum
# # Fill null values with 0
# # sub = sub.fillna({"PlusMinusChange": 0})

# # Calculate the sum of the 'PlusMinusChange' column
# sum_plus_minus_change = df.agg(sum("PlusMinusChange")).collect()[0][0]

# # Print the sum
# print(f"Sum of 'PlusMinusChange': {sum_plus_minus_change}")

# COMMAND ----------

# excluded_rows = games.join(player_games, join_condition, "left_anti")
# display(excluded_rows)

# COMMAND ----------

# unique_count = games.select("timestamp").distinct().count()
# # Display the count of unique values
# print(f"Number of unique values in column 'column_name': {unique_count}")


# # desired_order = ["HomeTeamScoreChange", "column1", "column2"]
# # df_reordered = df.select([col(column) for column in desired_order])

# COMMAND ----------

# Original attempt

# import dlt
# from pyspark.sql.functions import col, lag, sum as _sum, when, coalesce
# from pyspark.sql.window import Window

# @dlt.table(
#     name="plus_minus",
#     comment="Table containing cumulative plus-minus metric for players"
# )
# def calculate_plus_minus():
#     # Load the player_games and games tables
#     player_games = dlt.read("jw_raw_player_games")
#     games = dlt.read("jw_raw_games")

#     # Calculate the change in scores
#     window_spec = Window.partitionBy("GameID").orderBy("timestamp")
#     games = games.withColumn("PrevHomeTeamScore", lag("HomeTeamScore").over(window_spec))
#     games = games.withColumn("PrevAwayTeamScore", lag("AwayTeamScore").over(window_spec))
#     games = games.withColumn("HomeTeamScoreChange", col("HomeTeamScore") - col("PrevHomeTeamScore"))
#     games = games.withColumn("AwayTeamScoreChange", col("AwayTeamScore") - col("PrevAwayTeamScore"))

#     # Calculate the change in minutes
#     player_games = player_games.withColumn("PrevMinutes", lag("Minutes").over(window_spec))
#     player_games = player_games.withColumn("MinutesChange", col("Minutes") - col("PrevMinutes"))

#     # Rename the timestamp column in one of the DataFrames to avoid ambiguity
#     games = games.withColumnRenamed("timestamp", "game_timestamp")

#     # Join the tables to calculate the plus-minus metric
#     plus_minus = player_games.join(games, "GameID")

#     # Determine if the player is on the HomeTeam or AwayTeam
#     plus_minus = plus_minus.withColumn(
#         "PlusMinusChange",
#         when(
#             col("TeamID") == col("HomeTeamID"),
#             col("HomeTeamScoreChange") - col("AwayTeamScoreChange")
#         ).otherwise(
#             col("AwayTeamScoreChange") - col("HomeTeamScoreChange")
#         )
#     )

#     # Calculate the cumulative plus-minus metric
#     cumulative_window_spec = Window.partitionBy("GameID", "PlayerID").orderBy("game_timestamp").rowsBetween(Window.unboundedPreceding, Window.currentRow)
#     plus_minus = plus_minus.withColumn(
#         "CumulativePlusMinus",
#         _sum("PlusMinusChange").over(cumulative_window_spec)
#     )

#     # Carry over the previous CumulativePlusMinus for players not playing in the current window
#     plus_minus = plus_minus.withColumn(
#         "CumulativePlusMinus",
#         coalesce(col("CumulativePlusMinus"), lag("CumulativePlusMinus").over(Window.partitionBy("GameID", "PlayerID").orderBy("game_timestamp")))
#     )

#     return plus_minus.select("PlayerID", "GameID", "CumulativePlusMinus", "game_timestamp")