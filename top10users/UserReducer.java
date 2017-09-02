package com.toptenusers;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;



public class UserReducer extends Reducer<Text,IntWritable,Text,IntWritable> {
	private static TreeMap<IntWritable,Text> countMap = new TreeMap<IntWritable,Text>();
	
	@Override
	public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException
	{
		int sum = 0;
		
		for(IntWritable val : values){
			int value = Integer.parseInt(val.toString());
			sum = sum + value;
		}
		UserReducer.countMap.put( new IntWritable(sum),new Text(key));
			//context.write(new Text(key),new IntWritable(sum));
		
	}
	
	@Override
    protected void cleanup(Context context) throws IOException, InterruptedException {

        //Map sortedMap = sortByValues(countMap);

        int counter = 0;
        for (IntWritable value: UserReducer.countMap.descendingKeySet()) {
            if (counter ++ == 10) {
                break;
            }
            context.write(new Text(UserReducer.countMap.get(value)),value);
        }
  }

}
