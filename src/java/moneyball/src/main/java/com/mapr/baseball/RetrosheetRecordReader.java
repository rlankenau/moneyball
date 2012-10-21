package com.mapr.baseball;

import java.io.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;


/* Reads a single game at a time */
public class RetrosheetRecordReader extends RecordReader<LongWritable, Text> {
	private FSDataInputStream filein;
	private LineReader in;
	private long start=0, end=0, pos=0;
	private int maxLineLength;
	private LongWritable key;
	private Text value;

	public void close() throws IOException {
		if (in != null) {
			in.close();
		}
	}

	public long getPos() throws IOException
	{
		return pos;
	}	

	public float getProgress() 
	{
		if(end-start == 0)
			return 0;
		return (float)pos/(float)(end-start);
	}

	public LongWritable getCurrentKey() {
		return key;
	}

	public Text getCurrentValue() {
		return value;
	}

	private boolean isStartLine(Text t) {
		/* Find the end of the first field */
		int fieldTerm = t.find(",");
		int idTerm = t.find("id");
		return (idTerm != -1 && fieldTerm != -1 && idTerm < fieldTerm);
	}

	public void initialize(InputSplit genericSplit, TaskAttemptContext context) throws IOException, InterruptedException {
		FileSplit split = (FileSplit) genericSplit;
		final Path file = split.getPath();
		Configuration conf = context.getConfiguration();
		FileSystem fs = file.getFileSystem(conf);

		start = split.getStart();
		end = start + split.getLength();
		filein = fs.open(split.getPath());

		this.maxLineLength = conf.getInt("mapred.linerecordreader.maxlength", Integer.MAX_VALUE);

		in = new LineReader(filein, conf);
		
		if(start!=0) {
			boolean recordStart = false;
			/* Read until we have an id line, and then rewind. */
			filein.seek(start-1);
			Text tmpTxt = new Text();
			
			while(!recordStart){
				int linelen = in.readLine(tmpTxt, this.maxLineLength);
				if(isStartLine(tmpTxt))
				{
					/* Found an ID record. */
					filein.seek(start);
					recordStart = true; 	
				} else {
					start += linelen;
				}
			}
		}
		this.pos = start;

	}

	public boolean nextKeyValue() throws IOException
	{
		Text line = new Text();
		Text newline = new Text("\n");

		if(key == null)
			key = new LongWritable();
		key.set(pos);
		
		if(value == null)
			value = new Text();
		value.clear();

		int newSize = 0;
		newSize = in.readLine(line, maxLineLength);
		value.append(line.getBytes(), 0, line.getLength());
		value.append(newline.getBytes(), 0, newline.getLength());
		this.pos+=newSize;
		if(newSize == 0) {
			/* If we got 0 bytes, we're at EOF and need to bail.
			   since we didn't get a first record, return nothing
			*/
			key = null;
			value = null;
			return false;
		}
		while(true) {
			line.clear();
			
			newSize = in.readLine(line, maxLineLength);
			if(isStartLine(line))
			{
				/* Rewind the inputstream to the start of this line */
				filein.seek(this.pos);
				/* Return the current version. */
				return true;
			} else {
				value.append(line.getBytes(), 0, line.getLength());
				value.append(newline.getBytes(), 0, newline.getLength());
				this.pos+=newSize;
			}
		}
	}

}
