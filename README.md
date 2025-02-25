# 2025_de_bootcamp_capstone

Historical data download link:
https://barttorvik.com/2023_all_advgames.json.gz

Example of game boxscore:
https://barttorvik.com/box.php?muid=Tennessee%20TechTCU11-13&year=2018

Random notes from investigation of Bart Torvik historical data:
numdate:"20171113",
datetext: "11-13", NOTE: this is a derivative of numdate, drop it
opstyle: "",ALWAYS BLANK IN 2018
quality"",ALWAYS BLANK IN 2018
win1: 0,ALWAYS 0 or 1 IN 2018
opponent: "TCU",351 unique values (teams) IN 2018, 362 in 2024
muid: "Tennessee TechTCU11-13", 5540 unique values for each game.  contains both teams and date, although spacing is haphazard -> drop it
win2: 0,ALWAYS 0 or 1 IN 2018
Min_per: 33.0,NO IDEA WHAT THIS IS, 58 UNIQUE DECIMAL VALUES in 2018, USUALLY WHOLE NUMBERS + .0 BETWEEN 1.0-58.0, BUT ALSO A 0.5 IN THERE
ORtg: 80.8,
Usage: 23.0, NOTE: In 2023 this goes up to ~ 321...not sure what this stat means
eFG: 46.2,
TS_per: 46.2,
ORB_per: 0.0,
DRB_per: 18.0,
AST_per: 25.6, NOTE: Has large negative values in 2018, don't think that makes sense -> fill negative values to 0
TO_per: 20.6,
dunksmade: 0,
dunksatt: 1,
rimmade: 1,
rimatt: 3,
midmade: 2,
midatt: 5,
twoPM: 3.0,
twoPA: 8.0,
TPM: 2.0,
TPA: 5.0,
FTM: 0.0,
FTA: 0.0,
bpm_rd: -3.7,
Obpm: -2.1650076691501443,
Dbpm: -1.5561380232485664,
bpm_net: -2.8,
pts: 12,
ORB: 0.0,
DRB: 4.0,
AST: 4.0,
TOV: 3.0,
STL: 1.0,
BLK: 0.0,
stl_per: 1.6,
blk_per: 0.0,
PF: 3.0,
possessions: 76.075, THIS CAN BE ROUNDED, don't know why it's a decimal
bpm: -3.7211456923987107,
sbpm: -2.817982169501147,
loc: "", ALWAYS BLANK STRING FOR 2018
tt: "TennesseeTech",351 unique values (teams) IN 2018
pp: "Kajon Mack",4672 UNIQUE VALUES IN 2018
inches: 75,
cls: "Sr", NOTE: HAS 'NONE' AS A VALUE IN 2018
pid: 24412,NOTE: 4706 UNIQUE VALUES (MORE THAN NUMBER OF UNIQUE PLAYER NAMES -> this case is when players change teams during the season, they get a new pid when that happens)
year: 2018

https://barttorvik.com/box.php?muid=Tennessee%20TechTCU11-13&year=2018