moneyball
==============

Pig code for analysis of baseball statistics

Scripts:

  - summarize\_at\_bats.pig: Generate a single statistic representing the number of times a ball was hit to a specific fielder.
  - summarize\_loader.pig: Parse all records, taking into account game context.  This script produces a highly denormalized version of the data, including data such as runners on base, fielders, field conditions, and current game and player statistics.  This script does everything summarize\_UDF.pig did, and more.
