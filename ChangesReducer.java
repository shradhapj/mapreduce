package com.changes;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ChangesReducer extends Reducer<Text,Text,Text, LongWritable>{
	
	@Override
	public void reduce(Text key, Iterable<Text> values, Context context){
		long sum = 0;
		
		for(Text val : values){
			long value = Long.parseLong(val.toString());
			sum = sum + value;
		}
		
		try {
			context.write(new Text("Amount of data "+key+" in characters : "),new LongWritable(sum));
		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		
	}

}
