package com.classify.users;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class ClassifyUserReducer extends Reducer<Text,IntWritable,Text,LongWritable>{

	public void reduce(Text key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {

		int sum = 0;
		
		
//iterating through the values corresponding to a particular key
		for(IntWritable val: values){
		
			sum = sum + 1;
		
		   
		} // end of for
		context.write(new Text("Total number of users "+key+" : "), new LongWritable(sum));
	}// end of Reduce

}
