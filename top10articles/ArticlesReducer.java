package com.toptenarticles;

import java.io.IOException;
import java.util.TreeMap;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

import com.toptenusers.UserReducer;

public class ArticlesReducer extends Reducer<Text,IntWritable,Text,IntWritable> {

	private static TreeMap<IntWritable,Text> countMap = new TreeMap<IntWritable,Text>();
	@Override
	public void reduce(Text key, Iterable<IntWritable> values, Context context){
int sum = 0;
		
		for(IntWritable val : values){
			int value = Integer.parseInt(val.toString());
			sum = sum + value;
		}
		ArticlesReducer.countMap.put( new IntWritable(sum),new Text(key));
			//context.write(new Text(key),new IntWritable(sum));
		
	}
	
	@Override
    protected void cleanup(Context context) throws IOException, InterruptedException {

        //Map sortedMap = sortByValues(countMap);

        int counter = 0;
        for (IntWritable value: ArticlesReducer.countMap.descendingKeySet()) {
            if (counter ++ == 10) {
                break;
            }
            context.write(new Text(ArticlesReducer.countMap.get(value)),value);
        }
  }

	}
