register 'maprfs:///user/rlankenau/moneyball-1.0-SNAPSHOT.jar';
DEFINE RetrosheetLoader com.mapr.baseball.RetrosheetLoader();

set default_parallel 20;
set job.name mapr_baseball_summary
raw = LOAD '/projects/baseball/*.EV?' USING RetrosheetLoader();
describe raw;
-- This outputs a single record for each play, with all associated game information.
flattened = FOREACH raw GENERATE $0 .. $39, FLATTEN($40);

hr_only = FILTER flattened BY play_result=='Home run';

hr_by_batter = GROUP hr_only BY batter;

hr_count = FOREACH hr_by_batter GENERATE group as batter, COUNT(hr_only) as home_runs;

result = ORDER hr_count BY home_runs DESC;

STORE result INTO '/projects/home_runs/' USING PigStorage(',');

