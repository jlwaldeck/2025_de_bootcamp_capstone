# 2025_de_bootcamp_capstone

Problem Description/Project Purpose
The ultimate purpose of this project is to provide a visualization to display key metrics (both live and historical) which are indicative of a player’s contribution (whether positive or negative) to their team’s win/loss result in a game. Coaches use these types of analytics to make decisions before, during, and after each game in hopes of leading their teams to success. Fans use these types of analytics for many purposes, including ranting on Twitter while blaming coaches that make way too much money to make bad decisions.

One coaching decision topic that is hotly debated/tweeted by fan bases is based on player minutes (i.e., the number of minutes each player is given to play during a game). One metric that is popular in fan rants is the “Plus-Minus.”

The plus-minus metric in basketball is a statistic that measures the point differential when a player is on the court. Essentially, it tracks how the team performs in terms of scoring while the player is playing compared to when they're not. Here's a quick breakdown:

Positive Plus-Minus: Indicates that the team scored more points than the opponent while the player was on the court.

Negative Plus-Minus: Indicates that the team was outscored by the opponent while the player was on the court.

For example, if a player has a +5 plus-minus, it means their team scored 5 more points than the opponent while that player was on the floor. Conversely, a -3 means their team was outscored by 3 points during their time on the court. Note that players periodically transition from playing to sitting on the bench to playing again, etc. The plus-minus metric can only be calculated correctly by accounting for which players are on the court during each change in score during the entire game.

This metric, while imperfect, is valuable because it provides a more holistic view of a player's impact beyond just their individual stats (like points scored, rebounds, etc.). It's often used by coaches and analysts to assess a player's overall contribution to the team's success.

There are websites and apps dedicated to the calculation of these metrics, but each has its own distinct, undesirable flair. Most of the good ones require a paid subscription. The free ones are buggy and/or littered with unwanted ads. Few offer the customizability to meet the “one-stop-shop” desires of the individual fan while providing a simple, fan-focused analytical experience.

Personal motivations also should be disclosed. As a fan of Illinois basketball, I’ve observed that in each of the past 5 seasons, there is always one player that the coach seems to show favoritism towards and thus award that player more minutes than deserved. This project provides a free, customizable analytical vehicle for me to either:

Rant more intelligently. This project can add data-enriched fuel to my unquenchable flame of complaints re: player minute decisions.

Relax more. Stop being such a crazy/angry fan. Occasionally. If the data convinces me that my intuition is wrong, I can (theoretically) be at peace with the basketball universe. And I can do it in near-real-time!

Project Scope
Given the allotment of 2 weeks to complete this project, I chose to focus singularly on the plus-minus metric for the upstream dashboard.

Scope will include 2 primary analysis components: historical data and live data.

Note: Different and completely independent sources will be used for live vs. historical data.

Data infrastructure will be built to enable future expansion and customization of metrics, but in order to meet 3/2/25 DataExpert.IO bootcamp deadlines, scope of the resulting dashboard will be limited. This is a single-person project and scope creep will not be tolerated!

Data Sources and Tech Stack
General Notes/Constraints
I made a choice not to pay for my data sources. Why?

I’m generally frugal by nature.

I wanted to add a fun challenge. After investigating free trials for a few sites (e.g., Sportradar.com) and unsuccessfully haggling for special “personal use only” access with others, I decided to accept the challenge of somehow doing this for free (as well as legally and ethically).

No web scraping. I’m not an expert web scraper and didn’t want to spend time becoming one for this project. For both reasons of efficiency/reliability and ethics, I chose to shun web scraping.

Data Sources
Live data: SportsData.io

Notes:

Finding a free + workable data source to enable near-real-time metrics for plus-minus analytics was NOT easy. I ultimately found a functional solution with the “replay” feature at SportsData.io.Users who register for a free account have access to multiple “replays” which simulate historical periods of activity in a near-real-time update manner. For example, by starting the replay from December 2, 2023 via the SportsData.IO website, I received an API key for this particular replay and was able to obtain freshly updated data every 60 seconds. For data engineering bootcamp students wishing to test out a sports data pipeline but unwilling to pay for an API, this is the best thing imaginable short of sitting shotgun in the Delorean with Marty McFly.

The replay from SportsData.ionot only provides data for free, it gives users the flexibility to set the start time for their live data stream. This feature proved extremely useful for testing and validation.

Historical: barttorvik.com

Discussion & Justification:

Getting historical data for free was easier. But I still didn’t want to scrape it or pay for it. Enter Bart Torvik. For avid college basketball fans who are also avid data gurus, Bart is basically Taylor Swift. Except his eras are stored timestamp-partitioned databases…and I don’t have to hear about who he’s dating every time I try to watch a football game. Bart is amazing and well-known in the college hoops universe for his no-frills, but amazing analytics. He discourages web scraping and other such nonsense by making most of his raw data available to curious quants for free. Within reason, he has a “just ask me - I’ll try to help” policy. So I did. And he did. Exchanging quick emails with Bart about logistics and schemas was definitely one of the highlights of this project for me.

Bart provided giant raw .json files which contained all player stats for all games in a given year. I chose to include such historical data starting in 2014.

Tech Stack
Databricks

I chose a strategy of “do everything in Databricks” for this project. Why? I had a subscription as a result of joining the DataExpert.IO bootcamp. I chose to make this project my “learn Databricks by doing” vehicle.

Delta tables

Raw data was converted to JSON and/or CSV, processed/transformed, and ultimately stored in delta tables. An appendable delta table was used for storing near-real-time data after each ~60 second refresh via the SportsData.IO replay API. Delta Live Tables were used for all other data storage, including the metrics that serve the downstream Databricks dashboard.

ETL/Orchestration

Databricks Workflows and Pipelines were used. The integrated nature of these tools made this process easy once I learned the mechanics. Many companies whine about the cost of Databricks, but I understand why some are willing to pay a premium for these features. The UI was generally friendly and the overall experience generally positive. The monitoring features and integration of data quality checks were especially convenient.

Python/SQL

All code is written in either Python or SQL. These are not only my favorite languages; they are also the syntax of choice for working with Delta Lakes and data pipelines in Databricks.

Data Quality Checks

Databricks-integrated checks:

Multiple checks are included for each data source via Expectations. This makes it easy to deal with null values, etc. when writing rows to a Delta Live Table. The default setting for these checks is to “Warn” but users also have the option to drop corresponding rows or cause the job to fail.

Historical data download link:
https://barttorvik.com/2023_all_advgames.json.gz

Example of game boxscore:
https://barttorvik.com/box.php?muid=Tennessee%20TechTCU11-13&year=2018

Capstone Requirments:
https://github.com/DataExpert-io/analytics-engineer-capstone-project?tab=readme-ov-file#-project-submission-instructions