register 'maprfs:///user/rlankenau/moneyball-1.0-SNAPSHOT.jar';
DEFINE RetrosheetLoader com.mapr.baseball.RetrosheetLoader();

set default_parallel 20;
set job.name mapr_baseball_summary
raw = LOAD '/projects/baseballdata/*.EV?' USING RetrosheetLoader();
describe raw;
flattened = FOREACH raw GENERATE $0 .. $39, FLATTEN($40);

STORE flattened INTO '/projects/baseball_results/output_new/' USING PigStorage(',');

