package com.mapr.baseball;

import java.util.*;
import java.util.regex.*;
import java.io.*;

import org.apache.pig.ResourceSchema;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.pig.*;
import org.apache.pig.data.*;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.*;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;

/* Structure of records output by this loader:

{
	Game ID
	Park ID
	Game day
	Game month
	Game year
	day/night
	start time
	Game of day (1,2)
	Part of double-header (t/f)
	home team
	away team
	designated_hitter used (t/f)

	ump_home
	ump_1st
	ump_2nd
	ump_3rd
	ump_lf
	ump_rf

	winning_pitcher
	losing_pitcher

	how_scored
	scorer
	game recorder
	game translator
	contains pitches

	wind direction
	temperature
	sky
	wind_speed
	field_condition
	precip
	attendence
	
	final home score
	final away score
	winner
	events in game
	batters in game
	
	events
	[
		inning
		top/bottom
		at-bat of game
		event of game
		
		pitcher
		catcher
		1st
		2nd
		3rd
		ss
		left
		center
		right
		
		1st runner
		2nd runner
		3rd runner
		number on base

		batter
		at-bat for batter
		position played by batter
		count
		hits so far
		hit by pitch so far
		walks so far
		outs so far

		number of batters pitched to by current pitcher
		hits allowed so far
		walks so far
		wild pitches so far
		batters hit by pitch so far

		current home score
		current visitor score
		
		bases scored
		runs batted in previously
		runs batted in this play
		fielders handling ball []
		pitches[]

	]
}
*/

public class RetrosheetLoader extends LoadFunc implements LoadMetadata {

	public static int PLAY_INNING = 0;
	public static int PLAY_INNING_HALF = 1;
	public static int PLAY_ATBAT_OF_GAME = 2;
	public static int PLAY_EVENT_OF_GAME = 3;
	public static int PLAY_PITCHER = 4;
	public static int PLAY_CATCHER = 5;
	public static int PLAY_FIRST_BASEMAN = 6;
	public static int PLAY_SECOND_BASEMAN = 7;
	public static int PLAY_THIRD_BASEMAN = 8;
	public static int PLAY_SHORTSTOP = 9;
	public static int PLAY_LEFTFIELDER = 10;
	public static int PLAY_CENTERFIELDER = 11;
	public static int PLAY_RIGHTFIELDER = 12;
	public static int PLAY_DESIGNATED_HITTER = 13;
	public static int PLAY_RUNNER_ON_FIRST = 14;
	public static int PLAY_RUNNER_ON_SECOND = 15;
	public static int PLAY_RUNNER_ON_THIRD = 16;
	public static int PLAY_RUNNERS_ON_BASE = 17;
	public static int PLAY_CURRENT_BATTER = 18;
	public static int PLAY_CURRENT_BATTER_AT_BAT= 19;
	public static int PLAY_BATTER_POSITION = 20;
	public static int PLAY_COUNT = 21;
	public static int PLAY_BATTER_HITS_SO_FAR = 22;
	public static int PLAY_BATTER_HBP_SO_FAR = 23;
	public static int PLAY_BATTER_WALKS_SO_FAR = 24;
	public static int PLAY_BATTER_OUTS_SO_FAR = 25;
	public static int PLAY_PITCHER_BATTERS_PITCHED_TO = 26;
	public static int PLAY_PITCHER_HITS_ALLOWED = 27;
	public static int PLAY_PITCHER_WALKS_ALLOWED = 28;
	public static int PLAY_PITCHER_WILD_PITCHES = 29;
	public static int PLAY_PITCHER_BATTERS_BEANED = 30;
	public static int PLAY_PITCHER_STRIKEOUTS = 31;
	public static int PLAY_HOME_SCORE = 32;
	public static int PLAY_AWAY_SCORE = 33;
	public static int PLAY_BATTER_RBIS = 34;
	public static int PLAY_RESULT = 35;
	public static int PLAY_RBIS_ON_PLAY = 36;

	

	public static int GAME_ID = 0;
	public static int GAME_SITE = 1;
	public static int GAME_DATE_DAY = 2;
	public static int GAME_DATE_MONTH = 3;
	public static int GAME_DATE_YEAR = 4;
	public static int GAME_DAY_NIGHT = 5;
	public static int GAME_START_HOUR = 6;
	public static int GAME_START_MINUTES = 7;
	public static int GAME_OF_DAY = 8;
	public static int GAME_IS_DOUBLE_HEADER = 9;
	public static int GAME_HOME_TEAM= 10;
	public static int GAME_AWAY_TEAM= 11;
	public static int GAME_USE_DESIGNATED_HITTER = 12;
	public static int GAME_HOME_UMPIRE = 13;
	public static int GAME_1ST_BASE_UMPIRE = 14;
	public static int GAME_2ND_BASE_UMPIRE = 15;
	public static int GAME_3RD_BASE_UMPIRE = 16;
	public static int GAME_LEFT_FIELD_UMPIRE = 17;
	public static int GAME_RIGHT_FIELD_UMPIRE = 18;
	public static int GAME_WINNING_PITCHER = 19;
	public static int GAME_LOSING_PITCHER = 20;
	public static int GAME_HOW_SCORED= 21;
	public static int GAME_SCORER = 22;
	public static int GAME_INPUTTER = 23;
	public static int GAME_TRANSLATOR= 24;
	public static int GAME_HAS_PITCHES = 25;
	public static int GAME_WIND_DIRECTION = 26;
	public static int GAME_WIND_SPEED = 27;
	public static int GAME_TEMPERATURE = 28;
	public static int GAME_SKY_CONDITION= 29;
	public static int GAME_FIELD_CONDITION= 30;
	public static int GAME_PRECIPITATION = 31;
	public static int GAME_ATTENDANCE = 32;
	public static int GAME_FINAL_HOME_SCORE = 33;
	public static int GAME_FINAL_AWAY_SCORE = 34;
	public static int GAME_WINNER = 35;
	public static int GAME_EVENTS_IN_GAME = 36;
	public static int GAME_BATTERS_IN_GAME = 37;
	public static int GAME_DURATION= 38;
	public static int GAME_COUNTED_AS_SAVE = 39;
	public static int GAME_EVENTS = 40;

	Pattern event_pattern;
	class RetrosheetPlayer {

		/* General info */
		public String player_id;
		public String player_name;
		public boolean home_team;
		public int position;
		public int batting_order;

		/* Batter stats */
		public int rbis;
		public int hits_so_far;
		public int walks_so_far;
		public int outs_so_far;
		public int at_bat_number;
		public int hbp_so_far;
		public int strikeouts_so_far;

		/* Pitcher stats */
		public int batters_pitched_to;
		public int pitcher_walks_allowed;
		public int pitcher_hits_allowed;
		public int pitcher_wild_pitches;
		public int pitcher_beans; 
		public int pitcher_strikeouts;

		public RetrosheetPlayer(String[] record) {
			player_id = record[1].trim();	
			player_name = record[2].trim();
			home_team = Integer.parseInt(record[3])==0?true:false;
			batting_order = Integer.parseInt(record[4]);
			position = Integer.parseInt(record[5]);
		}


	}

	private RecordReader<LongWritable, Text> reader;
	private BagFactory bagFactory;
	private TupleFactory tupleFactory;

	public RetrosheetLoader(){
		bagFactory = BagFactory.getInstance();
		tupleFactory = TupleFactory.getInstance();	
	}

	public ResourceStatistics getStatistics(String location,
                                 org.apache.hadoop.mapreduce.Job job)
                                 throws IOException
	{
		return null;
	}

	public void setPartitionFilter(Expression partitionFilter)
	{}

	public String[] getPartitionKeys(String location,
                          org.apache.hadoop.mapreduce.Job job)
                          throws IOException
	{ 
		return null;
	}

	public ResourceSchema getSchema(java.lang.String str, org.apache.hadoop.mapreduce.Job job) {
		FieldSchema[] playFields = new FieldSchema[37];

		playFields[RetrosheetLoader.PLAY_INNING] = new FieldSchema("inning", org.apache.pig.data.DataType.INTEGER);
		playFields[RetrosheetLoader.PLAY_INNING_HALF] = new FieldSchema("half_of_inning", org.apache.pig.data.DataType.INTEGER);
		playFields[RetrosheetLoader.PLAY_ATBAT_OF_GAME] = new FieldSchema("atbat_of_game", org.apache.pig.data.DataType.INTEGER);
		playFields[RetrosheetLoader.PLAY_EVENT_OF_GAME] = new FieldSchema("event_of_game", org.apache.pig.data.DataType.INTEGER);
		playFields[RetrosheetLoader.PLAY_PITCHER] = new FieldSchema("pitcher", org.apache.pig.data.DataType.CHARARRAY);
		playFields[RetrosheetLoader.PLAY_CATCHER] = new FieldSchema("catcher", org.apache.pig.data.DataType.CHARARRAY);
		playFields[RetrosheetLoader.PLAY_FIRST_BASEMAN] = new FieldSchema("first_baseman", org.apache.pig.data.DataType.CHARARRAY);
		playFields[RetrosheetLoader.PLAY_SECOND_BASEMAN] = new FieldSchema("second_baseman", org.apache.pig.data.DataType.CHARARRAY);
		playFields[RetrosheetLoader.PLAY_THIRD_BASEMAN] = new FieldSchema("third_baseman", org.apache.pig.data.DataType.CHARARRAY);
		playFields[RetrosheetLoader.PLAY_SHORTSTOP] = new FieldSchema("shortstop", org.apache.pig.data.DataType.CHARARRAY);
		playFields[RetrosheetLoader.PLAY_LEFTFIELDER] = new FieldSchema("left_fielder", org.apache.pig.data.DataType.CHARARRAY);
		playFields[RetrosheetLoader.PLAY_CENTERFIELDER] = new FieldSchema("center_fielder", org.apache.pig.data.DataType.CHARARRAY);
		playFields[RetrosheetLoader.PLAY_RIGHTFIELDER] = new FieldSchema("right_fielder", org.apache.pig.data.DataType.CHARARRAY);
		playFields[RetrosheetLoader.PLAY_DESIGNATED_HITTER] = new FieldSchema("designated_hitter", org.apache.pig.data.DataType.CHARARRAY);
		playFields[RetrosheetLoader.PLAY_RUNNER_ON_FIRST] = new FieldSchema("runner_on_first", org.apache.pig.data.DataType.CHARARRAY);
		playFields[RetrosheetLoader.PLAY_RUNNER_ON_SECOND] = new FieldSchema("runner_on_second", org.apache.pig.data.DataType.CHARARRAY);
		playFields[RetrosheetLoader.PLAY_RUNNER_ON_THIRD] = new FieldSchema("runner_on_third", org.apache.pig.data.DataType.CHARARRAY);
		playFields[RetrosheetLoader.PLAY_RUNNERS_ON_BASE] = new FieldSchema("num_runners_on_base", org.apache.pig.data.DataType.INTEGER);
		playFields[RetrosheetLoader.PLAY_CURRENT_BATTER] = new FieldSchema("batter", org.apache.pig.data.DataType.CHARARRAY);
		playFields[RetrosheetLoader.PLAY_CURRENT_BATTER_AT_BAT] = new FieldSchema("batter_atbat", org.apache.pig.data.DataType.INTEGER);
		playFields[RetrosheetLoader.PLAY_BATTER_POSITION] = new FieldSchema("batter_position", org.apache.pig.data.DataType.INTEGER);
		playFields[RetrosheetLoader.PLAY_COUNT] = new FieldSchema("count", org.apache.pig.data.DataType.CHARARRAY);
		playFields[RetrosheetLoader.PLAY_BATTER_HITS_SO_FAR] = new FieldSchema("batter_hits_so_far", org.apache.pig.data.DataType.INTEGER);
		playFields[RetrosheetLoader.PLAY_BATTER_HBP_SO_FAR] = new FieldSchema("batter_hit_by_pitch_so_far", org.apache.pig.data.DataType.INTEGER);
		playFields[RetrosheetLoader.PLAY_BATTER_WALKS_SO_FAR] = new FieldSchema("batter_walks_so_far", org.apache.pig.data.DataType.INTEGER);
		playFields[RetrosheetLoader.PLAY_BATTER_OUTS_SO_FAR] = new FieldSchema("batter_outs_so_far", org.apache.pig.data.DataType.INTEGER);
		playFields[RetrosheetLoader.PLAY_PITCHER_BATTERS_PITCHED_TO] = new FieldSchema("pitcher_batters_pitched_to", org.apache.pig.data.DataType.INTEGER);
		playFields[RetrosheetLoader.PLAY_PITCHER_HITS_ALLOWED] = new FieldSchema("pitcher_hits_allowed", org.apache.pig.data.DataType.INTEGER);
		playFields[RetrosheetLoader.PLAY_PITCHER_WALKS_ALLOWED] = new FieldSchema("pitcher_walks_allowed", org.apache.pig.data.DataType.INTEGER);
		playFields[RetrosheetLoader.PLAY_PITCHER_WILD_PITCHES] = new FieldSchema("pitcher_wild_pitches", org.apache.pig.data.DataType.INTEGER);
		playFields[RetrosheetLoader.PLAY_PITCHER_BATTERS_BEANED] = new FieldSchema("pitcher_batters_beaned", org.apache.pig.data.DataType.INTEGER);
		playFields[RetrosheetLoader.PLAY_PITCHER_STRIKEOUTS] = new FieldSchema("pitcher_strikeouts", org.apache.pig.data.DataType.INTEGER);
		playFields[RetrosheetLoader.PLAY_HOME_SCORE] = new FieldSchema("current_home_score", org.apache.pig.data.DataType.INTEGER);
		playFields[RetrosheetLoader.PLAY_AWAY_SCORE] = new FieldSchema("current_away_score", org.apache.pig.data.DataType.INTEGER);
		playFields[RetrosheetLoader.PLAY_BATTER_RBIS] = new FieldSchema("batter_rbis", org.apache.pig.data.DataType.INTEGER);
		playFields[RetrosheetLoader.PLAY_RESULT] = new FieldSchema("play_result", org.apache.pig.data.DataType.CHARARRAY);
		playFields[RetrosheetLoader.PLAY_RBIS_ON_PLAY] = new FieldSchema("rbis_on_play", org.apache.pig.data.DataType.INTEGER);

		FieldSchema[] gameFields = new FieldSchema[41];
		
		gameFields[RetrosheetLoader.GAME_ID] = new FieldSchema("game_id", org.apache.pig.data.DataType.CHARARRAY);
		gameFields[RetrosheetLoader.GAME_SITE] = new FieldSchema("game_site_code", org.apache.pig.data.DataType.CHARARRAY);
		gameFields[RetrosheetLoader.GAME_DATE_DAY] = new FieldSchema("game_date_day", org.apache.pig.data.DataType.INTEGER);
		gameFields[RetrosheetLoader.GAME_DATE_MONTH] = new FieldSchema("game_date_month", org.apache.pig.data.DataType.INTEGER);
		gameFields[RetrosheetLoader.GAME_DATE_YEAR] = new FieldSchema("game_date_year", org.apache.pig.data.DataType.INTEGER);
		gameFields[RetrosheetLoader.GAME_DAY_NIGHT] = new FieldSchema("game_day_or_night", org.apache.pig.data.DataType.CHARARRAY);
		gameFields[RetrosheetLoader.GAME_START_HOUR] = new FieldSchema("game_start_hour", org.apache.pig.data.DataType.INTEGER);
		gameFields[RetrosheetLoader.GAME_START_MINUTES] = new FieldSchema("game_start_minutes", org.apache.pig.data.DataType.INTEGER);
		gameFields[RetrosheetLoader.GAME_OF_DAY] = new FieldSchema("game_of_day", org.apache.pig.data.DataType.INTEGER);
		gameFields[RetrosheetLoader.GAME_IS_DOUBLE_HEADER] = new FieldSchema("game_is_double_header", org.apache.pig.data.DataType.CHARARRAY);
		gameFields[RetrosheetLoader.GAME_HOME_TEAM] = new FieldSchema("game_home_team", org.apache.pig.data.DataType.CHARARRAY);
		gameFields[RetrosheetLoader.GAME_AWAY_TEAM] = new FieldSchema("game_away_team", org.apache.pig.data.DataType.CHARARRAY);
		gameFields[RetrosheetLoader.GAME_USE_DESIGNATED_HITTER] = new FieldSchema("game_uses_designated_hitter", org.apache.pig.data.DataType.CHARARRAY);
		gameFields[RetrosheetLoader.GAME_HOME_UMPIRE] = new FieldSchema("game_home_umpire", org.apache.pig.data.DataType.CHARARRAY);
		gameFields[RetrosheetLoader.GAME_1ST_BASE_UMPIRE] = new FieldSchema("game_1st_base_ump", org.apache.pig.data.DataType.CHARARRAY);
		gameFields[RetrosheetLoader.GAME_2ND_BASE_UMPIRE] = new FieldSchema("game_2nd_base_ump", org.apache.pig.data.DataType.CHARARRAY);
		gameFields[RetrosheetLoader.GAME_3RD_BASE_UMPIRE] = new FieldSchema("game_3rd_base_ump", org.apache.pig.data.DataType.CHARARRAY);
		gameFields[RetrosheetLoader.GAME_LEFT_FIELD_UMPIRE] = new FieldSchema("game_left_field_ump", org.apache.pig.data.DataType.CHARARRAY);
		gameFields[RetrosheetLoader.GAME_RIGHT_FIELD_UMPIRE] = new FieldSchema("game_right_field_ump", org.apache.pig.data.DataType.CHARARRAY);
		gameFields[RetrosheetLoader.GAME_WINNING_PITCHER] = new FieldSchema("game_winning_pitcher", org.apache.pig.data.DataType.CHARARRAY);
		gameFields[RetrosheetLoader.GAME_LOSING_PITCHER] = new FieldSchema("game_losing_pitcher", org.apache.pig.data.DataType.CHARARRAY);
		gameFields[RetrosheetLoader.GAME_HOW_SCORED] = new FieldSchema("game_how_scored", org.apache.pig.data.DataType.CHARARRAY);
		gameFields[RetrosheetLoader.GAME_SCORER] = new FieldSchema("game_scorer", org.apache.pig.data.DataType.CHARARRAY);
		gameFields[RetrosheetLoader.GAME_INPUTTER] = new FieldSchema("game_inputter", org.apache.pig.data.DataType.CHARARRAY);
		gameFields[RetrosheetLoader.GAME_TRANSLATOR] = new FieldSchema("game_translator", org.apache.pig.data.DataType.CHARARRAY);
		gameFields[RetrosheetLoader.GAME_HAS_PITCHES] = new FieldSchema("game_has_pitches", org.apache.pig.data.DataType.CHARARRAY);
		gameFields[RetrosheetLoader.GAME_WIND_DIRECTION] = new FieldSchema("game_wind_direction", org.apache.pig.data.DataType.CHARARRAY);
		gameFields[RetrosheetLoader.GAME_WIND_SPEED] = new FieldSchema("game_wind_speed", org.apache.pig.data.DataType.INTEGER);
		gameFields[RetrosheetLoader.GAME_TEMPERATURE] = new FieldSchema("game_temperature", org.apache.pig.data.DataType.INTEGER);
		gameFields[RetrosheetLoader.GAME_SKY_CONDITION] = new FieldSchema("game_sky_condition", org.apache.pig.data.DataType.CHARARRAY);
		gameFields[RetrosheetLoader.GAME_FIELD_CONDITION] = new FieldSchema("game_field_condition", org.apache.pig.data.DataType.CHARARRAY);
		gameFields[RetrosheetLoader.GAME_PRECIPITATION] = new FieldSchema("game_precipitation", org.apache.pig.data.DataType.CHARARRAY);
		gameFields[RetrosheetLoader.GAME_ATTENDANCE] = new FieldSchema("game_attendance", org.apache.pig.data.DataType.INTEGER);
		gameFields[RetrosheetLoader.GAME_FINAL_HOME_SCORE] = new FieldSchema("game_final_home_score", org.apache.pig.data.DataType.INTEGER);
		gameFields[RetrosheetLoader.GAME_FINAL_AWAY_SCORE] = new FieldSchema("game_final_away_score", org.apache.pig.data.DataType.INTEGER);
		gameFields[RetrosheetLoader.GAME_WINNER] = new FieldSchema("game_winner", org.apache.pig.data.DataType.CHARARRAY);
		gameFields[RetrosheetLoader.GAME_EVENTS_IN_GAME] = new FieldSchema("game_total_events_in_game", org.apache.pig.data.DataType.INTEGER);
		gameFields[RetrosheetLoader.GAME_BATTERS_IN_GAME] = new FieldSchema("game_total_batters_in_game", org.apache.pig.data.DataType.INTEGER);
		gameFields[RetrosheetLoader.GAME_DURATION] = new FieldSchema("game_duration", org.apache.pig.data.DataType.INTEGER);
		gameFields[RetrosheetLoader.GAME_COUNTED_AS_SAVE] = new FieldSchema("game_counted_as_save", org.apache.pig.data.DataType.CHARARRAY);
		FieldSchema eventsField =  new FieldSchema("game_events", org.apache.pig.data.DataType.BAG);
		FieldSchema eventsTuple = new FieldSchema("", org.apache.pig.data.DataType.TUPLE);
		eventsField.schema = new Schema(eventsTuple);
		eventsTuple.schema = new Schema(Arrays.asList(playFields));
		gameFields[RetrosheetLoader.GAME_EVENTS] = eventsField;
		

		return new ResourceSchema( new Schema(Arrays.asList(gameFields)) );
	}

	@Override
	public InputFormat getInputFormat() throws IOException {
		return new RetrosheetInputFormat();
	}

	@Override
	public void prepareToRead(RecordReader reader, PigSplit split) throws IOException
	{
		event_pattern = Pattern.compile("([KSDTW]|HR|WP|HP)?([1-9]+)?(?:/((?:SH)?B?[GLPF]+)?([1-9]+M?[XL]?[DS]?F?))?(?:\\.(.*))?");
		this.reader = reader;
	}

	@Override
	public void setLocation(String location, Job job) throws IOException
	{
		FileInputFormat.setInputPaths(job, location);
	}

	@Override
	public Tuple getNext() throws IOException {
		RetrosheetPlayer[] home_players = new RetrosheetPlayer[11];
		RetrosheetPlayer[] away_players = new RetrosheetPlayer[11];
		RetrosheetPlayer[] defense = null;
		Hashtable players = new Hashtable();
		int current_outs = 0;
		int home_score = 0;
		int away_score = 0;
		int atbat_of_game = 0;
		int event_of_game = 0;
		String last_batter = "";
		String current_batter = "";
		String runner_on_first = "";
		String runner_on_second = "";
		String runner_on_third = "";

		System.out.println("getNext()");

		Tuple game = tupleFactory.newTuple(41);		
		DataBag events = bagFactory.newDefaultBag();

		try {
			if(reader.nextKeyValue()) {
				LongWritable k = (LongWritable)reader.getCurrentKey();
				Text v = (Text)reader.getCurrentValue();
				String record = v.toString();
				/* Tokenize based on newlines */
				for(String line : record.split("\n")) {
					try {
						String[] elems = line.split(",");
						String linetype = elems[0].trim();
						if (linetype.equals("com")) {
							/* Comment.  Skip for now */
						} else if (linetype.equals("id")) {
							/* ID record.  If we've set the ID, this is an error */
							/* Raw game id */
							game.set(RetrosheetLoader.GAME_ID, elems[1]);
							/* We can get the home team from the ID */
							game.set(RetrosheetLoader.GAME_HOME_TEAM, elems[1].substring(0,3));
							/* Day, Month, Year */
							game.set(RetrosheetLoader.GAME_DATE_DAY, Integer.parseInt(elems[1].substring(3,7)));
							game.set(RetrosheetLoader.GAME_DATE_MONTH, Integer.parseInt(elems[1].substring(7,9)));
							game.set(RetrosheetLoader.GAME_DATE_YEAR, Integer.parseInt(elems[1].substring(9,11)));
							int game_of_day = Integer.parseInt(elems[1].substring(11,12));
							switch(game_of_day) {
								case 0:
									/* First game of the day, not a double header */
									game.set(RetrosheetLoader.GAME_OF_DAY, 1);
									game.set(RetrosheetLoader.GAME_IS_DOUBLE_HEADER, "no");
									break;
								case 1:
									game.set(RetrosheetLoader.GAME_OF_DAY, 1);
									game.set(RetrosheetLoader.GAME_IS_DOUBLE_HEADER, "yes");
									break;
								case 2:
									game.set(RetrosheetLoader.GAME_OF_DAY, 2);
									game.set(RetrosheetLoader.GAME_IS_DOUBLE_HEADER, "yes");
									break;
							}
						} else if (linetype.equals("start")) {
							/* Player start record.  Add to the current players list */
							try {
								RetrosheetPlayer p = new RetrosheetPlayer(elems);
								players.put(p.player_id, p);
								if(p.home_team) {
									home_players[p.position] = p;
								} else {
									away_players[p.position] = p;
								}
							} catch (Exception e) {
								/*TODO: Log this */
							}
						} else if (linetype.equals("sub")) {
							/* Player substitution.  Replace the player in the list now */
							try {
								RetrosheetPlayer p = new RetrosheetPlayer(elems);
								players.put(p.player_id, p);
								if(p.home_team) {
									home_players[p.position] = p;
								} else {
									away_players[p.position] = p;
								}
							} catch (Exception e) {
								/*TODO: Log this */
							}
						} else if (linetype.equals("play")) {
							try{
								/* Play.  Emit an event into the events list, update players on base, update score. */
								Tuple currentPlay = tupleFactory.newTuple(37);
								int possible_rbis = 0;

								/* We can set event of game now.  at-bat has to wait until we parse out stolen bases, etc. */
								event_of_game++;
								currentPlay.set(RetrosheetLoader.PLAY_EVENT_OF_GAME, event_of_game);
							
								/* Check if the batter has changed. */
								current_batter = elems[3].trim();
								RetrosheetPlayer current_player = (RetrosheetPlayer)players.get(current_batter);
								if(current_batter != last_batter)
								{
									atbat_of_game++;
								} 
								currentPlay.set(RetrosheetLoader.PLAY_ATBAT_OF_GAME, atbat_of_game);
								
								/* Set inning and whether it is top or bottom */
								currentPlay.set(RetrosheetLoader.PLAY_INNING, Integer.parseInt(elems[1]));
								if(elems[2].trim() == "0"){
									currentPlay.set(RetrosheetLoader.PLAY_INNING_HALF, "top");
									defense = away_players;
								} else {
									currentPlay.set(RetrosheetLoader.PLAY_INNING_HALF, "bottom");
									defense = home_players;
								}

								/* Set the fielders */	
								currentPlay.set(RetrosheetLoader.PLAY_PITCHER, defense[1].player_id);
								currentPlay.set(RetrosheetLoader.PLAY_CATCHER, defense[2].player_id);
								currentPlay.set(RetrosheetLoader.PLAY_FIRST_BASEMAN, defense[3].player_id);
								currentPlay.set(RetrosheetLoader.PLAY_SECOND_BASEMAN, defense[4].player_id);
								currentPlay.set(RetrosheetLoader.PLAY_THIRD_BASEMAN, defense[5].player_id);
								currentPlay.set(RetrosheetLoader.PLAY_SHORTSTOP, defense[6].player_id);
								currentPlay.set(RetrosheetLoader.PLAY_LEFTFIELDER, defense[7].player_id);
								currentPlay.set(RetrosheetLoader.PLAY_CENTERFIELDER, defense[8].player_id);
								currentPlay.set(RetrosheetLoader.PLAY_RIGHTFIELDER, defense[9].player_id);
								currentPlay.set(RetrosheetLoader.PLAY_DESIGNATED_HITTER, defense[10].player_id);
								
								/* Set the runners on base */
								currentPlay.set(RetrosheetLoader.PLAY_RUNNER_ON_FIRST, runner_on_first);
								currentPlay.set(RetrosheetLoader.PLAY_RUNNER_ON_SECOND, runner_on_second);
								currentPlay.set(RetrosheetLoader.PLAY_RUNNER_ON_THIRD, runner_on_third);
								int number_on_base = 0;
								if(!runner_on_first.equals(""))
									number_on_base++;
								if(!runner_on_second.equals(""))
									number_on_base++;
								if(!runner_on_third.equals(""))
									number_on_base++;
								currentPlay.set(RetrosheetLoader.PLAY_RUNNERS_ON_BASE, number_on_base);

								currentPlay.set(RetrosheetLoader.PLAY_CURRENT_BATTER, current_batter);
								current_player.at_bat_number++;
								currentPlay.set(RetrosheetLoader.PLAY_CURRENT_BATTER_AT_BAT, current_player.at_bat_number);
								currentPlay.set(RetrosheetLoader.PLAY_BATTER_POSITION, current_player.position);
								try {
									int count = Integer.parseInt(elems[4]);
									currentPlay.set(RetrosheetLoader.PLAY_COUNT, count/10 + "-" + count%10);
								} catch (Exception e) {
									currentPlay.set(RetrosheetLoader.PLAY_COUNT, "Unknown");
								}
								currentPlay.set(RetrosheetLoader.PLAY_BATTER_HITS_SO_FAR, current_player.hits_so_far);
								currentPlay.set(RetrosheetLoader.PLAY_BATTER_HBP_SO_FAR, current_player.hbp_so_far);
								currentPlay.set(RetrosheetLoader.PLAY_BATTER_WALKS_SO_FAR, current_player.walks_so_far);
								currentPlay.set(RetrosheetLoader.PLAY_BATTER_OUTS_SO_FAR, current_player.outs_so_far);
								currentPlay.set(RetrosheetLoader.PLAY_PITCHER_BATTERS_PITCHED_TO, defense[1].batters_pitched_to);
								currentPlay.set(RetrosheetLoader.PLAY_PITCHER_HITS_ALLOWED, defense[1].pitcher_hits_allowed);
								currentPlay.set(RetrosheetLoader.PLAY_PITCHER_WALKS_ALLOWED, defense[1].pitcher_walks_allowed);
								currentPlay.set(RetrosheetLoader.PLAY_PITCHER_WILD_PITCHES, defense[1].pitcher_wild_pitches);
								currentPlay.set(RetrosheetLoader.PLAY_PITCHER_BATTERS_BEANED, defense[1].pitcher_beans);
								currentPlay.set(RetrosheetLoader.PLAY_PITCHER_STRIKEOUTS, defense[1].pitcher_strikeouts);
								currentPlay.set(RetrosheetLoader.PLAY_HOME_SCORE, home_score);
								currentPlay.set(RetrosheetLoader.PLAY_AWAY_SCORE, away_score);
				
								/* Parse the event itself */
								Matcher m = event_pattern.matcher(elems[6]);				
								if(m.matches() != true) {
									System.err.println("Couldn't parse event data: " + elems[6]);
								} else {
									/* Figure out player movement so we can update everything in order */
									if(m.group(5) != null && !m.group(5).equals("")) {
										/* We have some player movement */
										String[] runner_mvmt = m.group(5).split(";");
										/* Scan the whole thing in case the movement is out of order */
										for(int i=3;i>0;i--) {
											for(int j=0;j<runner_mvmt.length;j++) {
												if(runner_mvmt[j].startsWith(""+i)) {
													/* Check if this is movement or an out. */
													if(runner_mvmt[j].substring(1,2).equals("X")) {
														/* Clear the runner */
														current_outs++;
														switch(i) {
															case 1:
																runner_on_first = "";
																break;
															case 2:
																runner_on_second = "";
																break;
															case 3:
																runner_on_third = "";
																break;
														}
													} else if (runner_mvmt[j].substring(1,2).equals("-")) {
														String newbasename = runner_mvmt[j].substring(2,3);
														if(newbasename.equals("H")) {
															/* Can't credit an RBI yet.  Save as conditional RBI */
															possible_rbis++;
															if(current_player.home_team) {
																home_score++;
															} else {
																away_score++;
															}
															switch(i) {
																case 1:
																	runner_on_first = "";
																	break;
																case 2:
																	runner_on_second = "";
																	break;
																case 3:
																	runner_on_third = "";
																	break;
															}
														} else {
															int newbase = Integer.parseInt(runner_mvmt[j].substring(2,3));
															String moving_runner = "";
															switch(i) {
																case 1:
																	moving_runner = runner_on_first;
																	runner_on_first = "";
						
																	break;
																case 2:
																	moving_runner = runner_on_second;
																	runner_on_second = "";
																	break;
																case 3:
																	moving_runner = runner_on_third;
																	runner_on_third = "";
																	break;
															}
															switch(newbase) {

																case 1:
																	
																	runner_on_first = moving_runner;
																	break;
																case 2:
																	runner_on_second = moving_runner;
																	break;
																case 3:
																	runner_on_second = moving_runner;
																	break;
															}
														}

													}
												}
											}
										}
									}
									currentPlay.set(RetrosheetLoader.PLAY_BATTER_RBIS, current_player.rbis);
									if(m.group(1) != null) {
										if(m.group(1).equals("S")) {
											runner_on_first = current_batter;	
											defense[1].pitcher_hits_allowed++;
											current_player.rbis+=possible_rbis;
											currentPlay.set(RetrosheetLoader.PLAY_RESULT, "Single");
										} else if (m.group(1).equals("D")) {
											runner_on_second = current_batter;	
											defense[1].pitcher_hits_allowed++;
											current_player.rbis+=possible_rbis;
											currentPlay.set(RetrosheetLoader.PLAY_RESULT, "Double");
										} else if (m.group(1).equals("T")) {
											runner_on_third = current_batter;	
											defense[1].pitcher_hits_allowed++;
											current_player.rbis+=possible_rbis;
											currentPlay.set(RetrosheetLoader.PLAY_RESULT, "Triple");
										} else if (m.group(1).equals("HR")) {
											if(current_player.home_team) {
												home_score++;
											} else {
												away_score++;
											}
											defense[1].pitcher_hits_allowed++;
											current_player.rbis+=possible_rbis;
											currentPlay.set(RetrosheetLoader.PLAY_RESULT, "Home run");
										} else if (m.group(1).equals("HP")) {
											runner_on_first = current_batter;
											defense[1].pitcher_beans++;
											current_player.hbp_so_far++;
											currentPlay.set(RetrosheetLoader.PLAY_RESULT, "Hit by pitch");
										} else if (m.group(1).equals("WP")) {
											defense[1].pitcher_wild_pitches++;
											currentPlay.set(RetrosheetLoader.PLAY_RESULT, "Wild pitch");
										} else if (m.group(1).equals("W")) {
											runner_on_first = current_batter;	
											defense[1].pitcher_walks_allowed++;
											currentPlay.set(RetrosheetLoader.PLAY_RESULT, "Walk");
										} else if (m.group(1).equals("K")) {
											defense[1].pitcher_strikeouts++;
											current_player.strikeouts_so_far++;
											currentPlay.set(RetrosheetLoader.PLAY_RESULT, "Strikeout");
										} else if (m.group(1).equals("")) {
											/* Out */
											current_player.outs_so_far++;
										}
									} else {
										/* Out */
										current_player.outs_so_far++;
									}
									/* Write out rbis and rbis_so_far. */
									currentPlay.set(RetrosheetLoader.PLAY_RBIS_ON_PLAY, possible_rbis);
									
								}
								events.add(currentPlay);
							} catch (Exception e) { 
								System.err.println("Error with play: " + e);
								e.printStackTrace();
							}	
						} else if (linetype.equals("version")) {
							/* File version info.  Skip for now */
						} else if (linetype.equals("info")) {
							/* Game info.  Add to the output tuple */
							String infotype = elems[1].trim();
							if (infotype.equals("hometeam")) {
								/* Ignore, this is already set by ID */
							} else if (infotype.equals("site")) {
								game.set(RetrosheetLoader.GAME_SITE, elems[2]);	
							} else if (infotype.equals("date")) {
								/* Already set in the ID */
							} else if (infotype.equals("number")) {
								/* Already set in the ID */
							} else if (infotype.equals("daynight")) {
								game.set(RetrosheetLoader.GAME_DAY_NIGHT, elems[2]);
							} else if (infotype.equals("starttime")) {
								String[] time_elems = elems[2].split(":");
								int hour = 0, minutes = 0;
								if(time_elems.length == 2) {
									hour = Integer.parseInt(time_elems[0]);
									minutes = Integer.parseInt(time_elems[1].substring(0,2));
									if(!time_elems[1].substring(2,4).equals("AM")) {
										hour+=12;
									}
								} else {
									if(elems[2].length() > 2) {
										int length = elems[2].length();
										minutes = Integer.parseInt(elems[2].substring(length-2, length));
										hour = Integer.parseInt(elems[2].substring(0, length-2));
									}
								}
								game.set(RetrosheetLoader.GAME_START_HOUR, hour);
								game.set(RetrosheetLoader.GAME_START_MINUTES, minutes);
							} else if( infotype.equals("visteam")){
								game.set(RetrosheetLoader.GAME_AWAY_TEAM, elems[2]);	
							} else if (infotype.equals("usedh")) {
								game.set(RetrosheetLoader.GAME_USE_DESIGNATED_HITTER, elems[2]);	
							} else if (infotype.equals("umphome")) {
								game.set(RetrosheetLoader.GAME_HOME_UMPIRE, elems[2]);	
							} else if (infotype.equals("ump1b")) {
								game.set(RetrosheetLoader.GAME_1ST_BASE_UMPIRE, elems[2]);	
							} else if (infotype.equals("ump2b")) {
								game.set(RetrosheetLoader.GAME_2ND_BASE_UMPIRE, elems[2]);	
							} else if (infotype.equals("ump3b")) {
								game.set(RetrosheetLoader.GAME_3RD_BASE_UMPIRE, elems[2]);	
							} else if (infotype.equals("umplf")) {
								game.set(RetrosheetLoader.GAME_LEFT_FIELD_UMPIRE, elems[2]);	
							} else if (infotype.equals("umprf")) {
								game.set(RetrosheetLoader.GAME_RIGHT_FIELD_UMPIRE, elems[2]);	
							} else if (infotype.equals("wp")) {
								game.set(RetrosheetLoader.GAME_WINNING_PITCHER, elems[2]);
							} else if (infotype.equals("lp")) {
								game.set(RetrosheetLoader.GAME_LOSING_PITCHER, elems[2]);
							} else if (infotype.equals("howscored")) {
								game.set(RetrosheetLoader.GAME_HOW_SCORED, elems[2]);
							} else if (infotype.equals("scorer")) {
								game.set(RetrosheetLoader.GAME_SCORER, elems[2]);
							} else if (infotype.equals("inputter")) {
								game.set(RetrosheetLoader.GAME_INPUTTER, elems[2]);
							} else if (infotype.equals("translator")) {
								game.set(RetrosheetLoader.GAME_TRANSLATOR, elems[2]);	
							} else if (infotype.equals("pitches")) {
								game.set(RetrosheetLoader.GAME_HAS_PITCHES, elems[2]);
							} else if (infotype.equals("winddir")) {
								game.set(RetrosheetLoader.GAME_WIND_DIRECTION, elems[2]);
							} else if (infotype.equals("windspeed")) {
								game.set(RetrosheetLoader.GAME_WIND_SPEED, Integer.parseInt(elems[2]));
							} else if (infotype.equals("temp")) {
								game.set(RetrosheetLoader.GAME_TEMPERATURE, Integer.parseInt(elems[2]));
							} else if (infotype.equals("sky")) {
								game.set(RetrosheetLoader.GAME_SKY_CONDITION, elems[2]);
							} else if (infotype.equals("fieldcond")) {
								game.set(RetrosheetLoader.GAME_FIELD_CONDITION, elems[2]);
							} else if (infotype.equals("precip")) {
								game.set(RetrosheetLoader.GAME_PRECIPITATION, elems[2]);	
							} else if (infotype.equals("attendance")) {
								game.set(RetrosheetLoader.GAME_ATTENDANCE, Integer.parseInt(elems[2]));
							} else if (infotype.equals("timeofgame")) {
								game.set(RetrosheetLoader.GAME_DURATION, Integer.parseInt(elems[2]));
							} else if (infotype.equals("save")) {
								game.set(RetrosheetLoader.GAME_COUNTED_AS_SAVE, elems[2]);
							}
						} else if (linetype.equals("data")) {
							/* Other game data.  Generally earned runs for the pitchers */
						}
					} catch (Exception e) {
						System.err.println("Malformed data: '" + line + "' exception: " + e);
					}
				}
				game.set(RetrosheetLoader.GAME_EVENTS, events);
				game.set(RetrosheetLoader.GAME_FINAL_HOME_SCORE, home_score);
				game.set(RetrosheetLoader.GAME_FINAL_AWAY_SCORE, away_score);
				game.set(RetrosheetLoader.GAME_EVENTS_IN_GAME, event_of_game);
				game.set(RetrosheetLoader.GAME_BATTERS_IN_GAME, atbat_of_game);
				if(home_score>away_score) {
					game.set(RetrosheetLoader.GAME_WINNER, game.get(RetrosheetLoader.GAME_HOME_TEAM));
				} else if (away_score>home_score) {
					game.set(RetrosheetLoader.GAME_WINNER, game.get(RetrosheetLoader.GAME_AWAY_TEAM));
				}
				return game;
			
			}
		} catch (Exception e) {
			/*TODO: Log this */
			throw new IOException("Error parsing", e);
		}
		return null;
		
	}
}
