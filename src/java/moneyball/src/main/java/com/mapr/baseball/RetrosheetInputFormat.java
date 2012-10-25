package com.mapr.baseball;
import java.lang.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

public class RetrosheetInputFormat extends TextInputFormat {

	public RecordReader<LongWritable, Text> createRecordReader(InputSplit split, TaskAttemptContext context) {
		return new RetrosheetRecordReader();
	}

}
