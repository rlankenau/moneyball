register 'maprfs:///user/rlankenau/moneyball-1.0-SNAPSHOT.jar';
DEFINE RetrosheetLoader com.mapr.baseball.RetrosheetLoader();

set default_parallel 20;
set job.name mapr_baseball_to_json;
raw = LOAD '/projects/baseballdata/*.EV?' USING RetrosheetLoader();

store raw INTO '/projects/baseball_results/output_raw_json/';

flattened = FOREACH raw GENERATE $0 .. $39, FLATTEN($40);

STORE flattened INTO '/projects/baseball_results/output_json/' using JsonStorage();

