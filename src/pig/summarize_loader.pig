register 'maprfs:///user/rlankenau/moneyball-1.0-SNAPSHOT.jar';
DEFINE RetrosheetLoader com.mapr.baseball.RetrosheetLoader();

set default_parallel 20;
set job.name mapr_baseball_summary
raw = LOAD '/projects/baseballdata/' USING RetrosheetLoader();

STORE raw INTO 'loader_output' USING PigStorage(',');
