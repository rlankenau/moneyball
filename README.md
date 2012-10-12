mapr-moneyball
==============

Pig code for analysis of baseball statistics

The current version of this script uses only built-in Pig features to parse raw baseball data from retrosheet.org and produce summary statistics.

TODO:
- Replace regex field transformation with UDF
- Introduce game-aware LoadFunc to allow per-game statistics to be calculated
- Add better documentation of data format
