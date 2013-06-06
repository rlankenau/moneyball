register 'maprfs:///user/rlankenau/moneyball-1.0-SNAPSHOT.jar';
DEFINE RetrosheetLoader com.mapr.baseball.RetrosheetLoader();

set default_parallel 20;
set job.name mapr_baseball_summary
raw = LOAD '/projects/baseball/*.EV{N,A}' USING RetrosheetLoader();
describe raw;
-- This outputs a single record for each play, with all associated game information.
flattened = FOREACH raw GENERATE $0 .. $39, FLATTEN($40);

STORE flattened INTO '/projects/baseball_summary/' USING PigStorage(',');

