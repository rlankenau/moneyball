package com.mapr.baseball;

import java.util.*;
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
		runs batted in
		runs batted in so far
		fielders handling ball []
		pitches[]

	]
}
*/

public class RetrosheetLoader extends LoadFunc {

	class RetrosheetPlayer {

		/* General info */
		public String player_id;
		public String player_name;
		public boolean home_team;
		public int position;
		public int batting_order;

		/* Batter stats */
		public int runs_batted_in_so_far;
		public int hits_so_far;
		public int walks_so_far;
		public int outs_so_far;
		public int at_bat_number;
		public int hit_by_pitch_so_far;
		public int batters_pitched_to;

		/* Pitcher stats */
		public int pitcher_walks_allowed;
		public int pitcher_hits_allowed;
		public int pitcher_wild_pitches;
		public int pitcher_beans; 
		public int pitcher_strikeouts;

		public RetrosheetPlayer(String[] record) {
			player_id = record[1];	
			player_name = record[2];
			home_team = Integer.parseInt(record[3])==0?true:false;
			batting_order = Integer.parseInt(record[4]);
			position = Integer.parseInt(record[5]);
		}


	}

	private RecordReader reader;
	private BagFactory bagFactory;
	private TupleFactory tupleFactory;

	public RetrosheetLoader(){
		bagFactory = BagFactory.getInstance();
		tupleFactory = TupleFactory.getInstance();	
	}

	public ResourceSchema getSchema() {
		List<FieldSchema> fieldSchemaList = new ArrayList<FieldSchema>();

		fieldSchemaList.add( new FieldSchema("game_id", org.apache.pig.data.DataType.CHARARRAY) );
		fieldSchemaList.add( new FieldSchema("park_id", org.apache.pig.data.DataType.CHARARRAY) );
		fieldSchemaList.add( new FieldSchema("game_date_day", org.apache.pig.data.DataType.INTEGER) );
		fieldSchemaList.add( new FieldSchema("game_date_month", org.apache.pig.data.DataType.INTEGER) );
		fieldSchemaList.add( new FieldSchema("game_date_year", org.apache.pig.data.DataType.INTEGER) );
		fieldSchemaList.add( new FieldSchema("day_night", org.apache.pig.data.DataType.CHARARRAY) );
		fieldSchemaList.add( new FieldSchema("start_time_hours", org.apache.pig.data.DataType.INTEGER) );
		fieldSchemaList.add( new FieldSchema("start_time_minutes", org.apache.pig.data.DataType.INTEGER) );
		fieldSchemaList.add( new FieldSchema("game_of_day", org.apache.pig.data.DataType.INTEGER) );
		fieldSchemaList.add( new FieldSchema("double_header", org.apache.pig.data.DataType.CHARARRAY) );
		fieldSchemaList.add( new FieldSchema("home_team", org.apache.pig.data.DataType.CHARARRAY) );
		fieldSchemaList.add( new FieldSchema("away_team", org.apache.pig.data.DataType.CHARARRAY) );
		fieldSchemaList.add( new FieldSchema("designated_hitter_used", org.apache.pig.data.DataType.CHARARRAY) );
		fieldSchemaList.add( new FieldSchema("umpire_home", org.apache.pig.data.DataType.CHARARRAY) );
		fieldSchemaList.add( new FieldSchema("umpire_1st_base", org.apache.pig.data.DataType.CHARARRAY) );
		fieldSchemaList.add( new FieldSchema("umpire_2nd_base", org.apache.pig.data.DataType.CHARARRAY) );
		fieldSchemaList.add( new FieldSchema("umpire_3rd_base", org.apache.pig.data.DataType.CHARARRAY) );
		fieldSchemaList.add( new FieldSchema("umpire_left_field", org.apache.pig.data.DataType.CHARARRAY) );
		fieldSchemaList.add( new FieldSchema("umpire_right_field", org.apache.pig.data.DataType.CHARARRAY) );
		fieldSchemaList.add( new FieldSchema("winning_pitcher", org.apache.pig.data.DataType.CHARARRAY) );
		fieldSchemaList.add( new FieldSchema("losing_pitcher", org.apache.pig.data.DataType.CHARARRAY) );
		fieldSchemaList.add( new FieldSchema("scoring_method", org.apache.pig.data.DataType.CHARARRAY) );
		fieldSchemaList.add( new FieldSchema("scorer", org.apache.pig.data.DataType.CHARARRAY) );
		fieldSchemaList.add( new FieldSchema("recorder", org.apache.pig.data.DataType.CHARARRAY) );
		fieldSchemaList.add( new FieldSchema("translator", org.apache.pig.data.DataType.CHARARRAY) );
		fieldSchemaList.add( new FieldSchema("pitches_recorded", org.apache.pig.data.DataType.CHARARRAY) );
		fieldSchemaList.add( new FieldSchema("wind_direction", org.apache.pig.data.DataType.CHARARRAY) );
		fieldSchemaList.add( new FieldSchema("wind_speed", org.apache.pig.data.DataType.CHARARRAY) );
		fieldSchemaList.add( new FieldSchema("temperature", org.apache.pig.data.DataType.CHARARRAY) );
		fieldSchemaList.add( new FieldSchema("sky", org.apache.pig.data.DataType.CHARARRAY) );
		fieldSchemaList.add( new FieldSchema("field_condition", org.apache.pig.data.DataType.CHARARRAY) );
		fieldSchemaList.add( new FieldSchema("precipitation", org.apache.pig.data.DataType.CHARARRAY) );
		fieldSchemaList.add( new FieldSchema("attendance", org.apache.pig.data.DataType.INTEGER) );
		fieldSchemaList.add( new FieldSchema("final_home_score", org.apache.pig.data.DataType.INTEGER) );
		fieldSchemaList.add( new FieldSchema("final_away_score", org.apache.pig.data.DataType.INTEGER) );
		fieldSchemaList.add( new FieldSchema("winning_team", org.apache.pig.data.DataType.CHARARRAY) );
		fieldSchemaList.add( new FieldSchema("events_in_game", org.apache.pig.data.DataType.INTEGER) );
		fieldSchemaList.add( new FieldSchema("batters_in_game", org.apache.pig.data.DataType.INTEGER) );
		fieldSchemaList.add( new FieldSchema("game_duration", org.apache.pig.data.DataType.INTEGER));
		fieldSchemaList.add( new FieldSchema("save", org.apache.pig.data.DataType.CHARARRAY) );
		fieldSchemaList.add( new FieldSchema("events", org.apache.pig.data.DataType.BAG) );


		return new ResourceSchema( new Schema(fieldSchemaList) );
	}

	@Override
	public InputFormat getInputFormat() throws IOException {
		return new RetrosheetInputFormat();
	}

	@Override
	public void prepareToRead(RecordReader reader, PigSplit split) 
	{

	}

	@Override
	public void setLocation(String location, Job job) throws IOException
	{
		FileInputFormat.setInputPaths(job, location);
	}

	@Override
	public Tuple getNext() throws IOException {
		RetrosheetPlayer[] home_players = new RetrosheetPlayer[10];
		RetrosheetPlayer[] away_players = new RetrosheetPlayer[10];
		int home_score;
		int away_score;

		Tuple game = tupleFactory.newTuple(41);		
		DataBag events = bagFactory.newDefaultBag();
		game.set(40, events);

		try {
			if(reader.nextKeyValue()) {
				LongWritable k = (LongWritable)reader.getCurrentKey();
				Text v = (Text)reader.getCurrentValue();
				String record = v.toString();
				/* Tokenize based on newlines */
				for(String line : record.split("\n")) {
					String[] elems = line.split(",");
					String linetype = elems[0].trim();
					if (linetype.equals("com")) {
						/* Comment.  Skip for now */
					} else if (linetype.equals("id")) {
						/* ID record.  If we've set the ID, this is an error */
						/* Raw game id */
						game.set(0, elems[1]);
						/* We can get the home team from the ID */
						game.set(10, elems[1].substring(0,4));
						/* Day, Month, Year */
						game.set(2, Integer.parseInt(elems[1].substring(4,8)));
						game.set(3, Integer.parseInt(elems[1].substring(8,10)));
						game.set(4, Integer.parseInt(elems[1].substring(10,12)));
						int game_of_day = Integer.parseInt(elems[1].substring(12,13));
						switch(game_of_day) {
							case 0:
								/* First game of the day, not a double header */
								game.set(8, 1);
								game.set(9, "no");
								break;
							case 1:
								game.set(8, 1);
								game.set(9, "yes");
								break;
							case 2:
								game.set(8, 2);
								game.set(9, "yes");
								break;
						}
					} else if (linetype.equals("start")) {
						/* Player start record.  Add to the current players list */
						try {
							RetrosheetPlayer p = new RetrosheetPlayer(elems);
							if(p.home_team){
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
							if(p.home_team){
								home_players[p.position] = p;
							} else {
								away_players[p.position] = p;
							}
						} catch (Exception e) {
							/*TODO: Log this */
						}
					} else if (linetype.equals("play")) {
						/* Play.  Emit an event into the events list, update players on base, update score. */
						
					} else if (linetype.equals("version")) {
						/* File version info.  Skip for now */
					} else if (linetype.equals("info")) {
						/* Game info.  Add to the output tuple */
						String infotype = elems[1].trim();
						if (infotype.equals("hometeam")) {
							/* Ignore, this is already set by ID */
						} else if (infotype.equals("site")) {
							game.set(1, elems[2]);	
						} else if (infotype.equals("date")) {
							/* Already set in the ID */
						} else if (infotype.equals("number")) {
							/* Already set in the ID */
						} else if (infotype.equals("daynight")) {
							game.set(5, elems[2]);
						} else if (infotype.equals("starttime")) {
							String[] time_elems = elems[2].split(",");
							int hour = Integer.parseInt(time_elems[0]);
							int minutes = Integer.parseInt(time_elems[1].substring(0,2));
							if(time_elems[1].substring(2,4).equals("PM")) {
								hour+=12;
							}
							game.set(6, hour);
							game.set(7, minutes);
						} else if( infotype.equals("visteam")){
							game.set(11, elems[2]);	
						} else if (infotype.equals("usedh")) {
							game.set(12, elems[2]);	
						} else if (infotype.equals("umphome")) {
							game.set(13, elems[2]);	
						} else if (infotype.equals("ump1b")) {
							game.set(14, elems[2]);	
						} else if (infotype.equals("ump2b")) {
							game.set(15, elems[2]);	
						} else if (infotype.equals("ump3b")) {
							game.set(16, elems[2]);	
						} else if (infotype.equals("umplf")) {
							game.set(17, elems[2]);
						} else if (infotype.equals("umprf")) {
							game.set(18, elems[2]);
						} else if (infotype.equals("wp")) {
							game.set(19, elems[2]);
						} else if (infotype.equals("lp")) {
							game.set(20, elems[2]);
						} else if (infotype.equals("howscored")) {
							game.set(21, elems[2]);
						} else if (infotype.equals("scorer")) {
							game.set(22, elems[2]);
						} else if (infotype.equals("inputter")) {
							game.set(23, elems[2]);
						} else if (infotype.equals("translator")) {
							game.set(24, elems[2]);	
						} else if (infotype.equals("pitches")) {
							game.set(25, elems[2]);
						} else if (infotype.equals("winddir")) {
							game.set(26, elems[2]);
						} else if (infotype.equals("windspeed")) {
							game.set(27, Integer.parseInt(elems[2]));
						} else if (infotype.equals("temp")) {
							game.set(28, Integer.parseInt(elems[2]));
						} else if (infotype.equals("sky")) {
							game.set(29, elems[2]);
						} else if (infotype.equals("fieldcond")) {
							game.set(30, elems[2]);
						} else if (infotype.equals("precip")) {
							game.set(31, elems[2]);	
						} else if (infotype.equals("attendance")) {
							game.set(32, Integer.parseInt(elems[2]));
						} else if (infotype.equals("timeofgame")) {
							game.set(38, Integer.parseInt(elems[2]));
						} else if (infotype.equals("save")) {
							game.set(39, elems[2]);
						}
					} else if (linetype.equals("data")) {
						/* Other game data.  Generally earned runs for the pitchers */
					}
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
