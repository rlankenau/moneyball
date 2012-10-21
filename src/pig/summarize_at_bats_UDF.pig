register 'maprfs:///user/rlankenau/moneyball-1.0-SNAPSHOT.jar';
DEFINE EventParser com.mapr.baseball.EventParser();

set default_parallel 20;
set job.name mapr_baseball_summary
raw = LOAD '/projects/baseballdata/' USING PigStorage(',');
SPLIT raw INTO players IF $0 MATCHES 'start' OR $0 MATCHES 'sub', plays IF $0 MATCHES 'play';

-- Extract all players
players_name_id = FOREACH players GENERATE $1 as id, $2 as name;
player_ids = distinct players_name_id;


atbats = FOREACH plays GENERATE $3 AS id:chararray, $6 AS result:chararray;
parsed_atbats = FOREACH atbats GENERATE id,result,EventParser(result) as parsed;

errors = FILTER parsed_atbats BY parsed.$0 MATCHES 'Error';

STORE errors INTO 'errors' USING PigStorage(',');
STORE parsed_atbats INTO 'udf_version' USING PigStorage(',');
