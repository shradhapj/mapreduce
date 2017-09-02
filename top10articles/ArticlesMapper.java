package com.toptenarticles;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ArticlesMapper extends Mapper<LongWritable,Text,Text,IntWritable> {

	@Override
	public void map(LongWritable key, Text value, Context context){
		try {
			String line = value.toString();
			String[] key_val = line.split(",");
			String title = key_val[2];
			System.out.println("User:::"+title);
			context.write(new Text(title), new IntWritable(1));
		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
}
