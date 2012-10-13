set default_parallel 5;
set job.name mapr_baseball_summary
raw = LOAD '$inputdir' USING PigStorage(',');
SPLIT raw INTO players IF $0 MATCHES 'start' OR $0 MATCHES 'sub', plays IF $0 MATCHES 'play';

-- Extract all players
players_name_id = FOREACH players GENERATE $1 as id, $2 as name;
player_ids = distinct players_name_id;

-- Write out all players for query through Hive
STORE player_ids INTO '$playerdir' using PigStorage(',');

atbats = FOREACH plays GENERATE $3 AS id, $6 AS result;
atbats_processed = FOREACH atbats GENERATE id, result as raw, REGEX_EXTRACT(result, '([^./]*)[/]?([^.]*)[.]?(.*)', 1) as result, REGEX_EXTRACT(result, '([^./]*)[/]?([^.]*)[.]?(.*)', 2) as description;

SPLIT atbats_processed INTO outs_tmp IF result MATCHES '(?!WP)(?!IW)(?!HR)(?!SB)[^SDTW].*', singles_tmp IF result MATCHES 'S(?!B).*', doubles_tmp IF result MATCHES 'D.*', triples_tmp IF result MATCHES 'T.*', hr_tmp IF result MATCHES 'HR.*', walks_tmp IF result MATCHES '(?!WP)(W|IW).*';

-- Extract result of at-bat
outs = FOREACH outs_tmp GENERATE id, raw, result, result as location, description, 'out' as bases, '0' as was_walk;
singles = FOREACH singles_tmp GENERATE id, raw, result, REGEX_EXTRACT(result, 'S(.*)', 1) as location, description, 'single' as bases, '0' as was_walk;
doubles = FOREACH doubles_tmp GENERATE id, raw, result, REGEX_EXTRACT(result, 'D(.*)', 1) as location, description, 'double' as bases, '0' as was_walk;
triples = FOREACH triples_tmp GENERATE id, raw, result, REGEX_EXTRACT(result, 'T(.*)', 1) as location, description, 'triple' as bases, '0' as was_walk;
homeruns = FOREACH hr_tmp GENERATE id, raw, result, REGEX_EXTRACT(result, 'HR(.*)', 1) as location, description, 'home run' as bases, '0' as was_walk;
walks = FOREACH walks_tmp GENERATE id, raw, result, REGEX_EXTRACT(result, '(W|IW)(.*)', 2) as location, description, 'walk' as bases, '1' as was_walk;

hits_parsed = UNION outs, singles, doubles, triples, homeruns, walks;

-- Extract fielder who handled the ball
SPLIT hits_parsed INTO loc_unknown_tmp IF location MATCHES '[^1-9].*', 
	                   loc_leftfield_tmp IF location MATCHES '7(?!8).*',
					   loc_leftcenter_tmp IF location MATCHES '78.*',
					   loc_centerfield_tmp IF location MATCHES '8(?!9).*',
					   loc_rightcenter_tmp IF location MATCHES '89.*',
					   loc_rightfield_tmp IF location MATCHES '9.*',
					   loc_thirdbase_tmp IF location MATCHES '5.*',
					   loc_shortstop_tmp IF location MATCHES '6.*',
					   loc_secondbase_tmp IF location MATCHES '4.*',
					   loc_firstbase_tmp IF location MATCHES '3.*',
					   loc_pitcher_tmp IF location MATCHES '1.*',
					   loc_catcher_tmp IF location MATCHES '2.*';

loc_unknown = FOREACH loc_unknown_tmp GENERATE *, 'unknown' as position;
loc_leftfield = FOREACH loc_leftfield_tmp GENERATE *, 'left field' as position;
loc_leftcenter = FOREACH loc_leftcenter_tmp GENERATE *, 'left center' as position;
loc_centerfield = FOREACH loc_centerfield_tmp GENERATE *, 'center field' as position;
loc_rightcenter = FOREACH loc_rightcenter_tmp GENERATE *, 'right center' as position;
loc_rightfield = FOREACH loc_rightfield_tmp GENERATE *, 'right field' as position;
loc_thirdbase = FOREACH loc_thirdbase_tmp GENERATE *, 'third base' as position;
loc_shortstop = FOREACH loc_shortstop_tmp GENERATE *, 'shortstop' as position;
loc_secondbase = FOREACH loc_secondbase_tmp GENERATE *, 'second base' as position;
loc_firstbase = FOREACH loc_firstbase_tmp GENERATE *, 'first base' as position;
loc_pitcher = FOREACH loc_pitcher_tmp GENERATE *, 'pitcher' as position;
loc_catcher = FOREACH loc_catcher_tmp GENERATE *, 'catcher' as position;

position_parsed = UNION loc_unknown, loc_leftfield, loc_leftcenter, loc_centerfield, loc_rightcenter, loc_rightfield, loc_thirdbase, loc_shortstop, loc_secondbase, loc_firstbase, loc_pitcher, loc_catcher;

-- Extract ball position
SPLIT position_parsed INTO short_tmp IF description MATCHES '.*S',
	                       deep_tmp IF description MATCHES '.*D',
						   normal_tmp IF description MATCHES '.*(?<!S)(?<!D)';
short = FOREACH short_tmp GENERATE *, 'short' as placement;
deep = FOREACH deep_tmp GENERATE *, 'deep' as placement;
normal = FOREACH normal_tmp GENERATE *, '' as placement;

final_plays = UNION short, deep, normal;

-- Generate counts

stripped_plays = FOREACH final_plays GENERATE id, position, placement, bases;

grouped_plays = GROUP stripped_plays BY (id,position,placement,bases);

counted_plays = FOREACH grouped_plays GENERATE group, COUNT($1);

STORE counted_plays INTO '$outputdir' USING PigStorage(',');

