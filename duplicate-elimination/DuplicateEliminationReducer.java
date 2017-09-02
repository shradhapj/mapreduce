package com.duplicate.elimination;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.HashMap;
import java.util.HashSet;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class DuplicationEliminationReducer extends Reducer<Text,IntWritable,Text,IntWritable> {
	public void reduce(Text key, Iterable<IntWritable> values, Context
			context ) throws IOException, InterruptedException {
		int sum = 0;
		for (IntWritable val : values) {
		sum += val.get();
		}
		context.write( key, new IntWritable(sum) );
		}

}
