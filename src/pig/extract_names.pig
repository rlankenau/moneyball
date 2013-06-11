set default_parallel 20;
set job.name 'Extract name->id' 
raw = LOAD '/projects/baseball/*.EV{N,A}' USING PigStorage(',') AS (type:chararray, id:chararray, name:chararray);

players = FILTER raw BY type MATCHES 'start' OR type MATCHES 'sub'; 

mapping = FOREACH players GENERATE id, name;

result = DISTINCT mapping;

STORE result INTO '/projects/name_to_id/' USING PigStorage(',');

