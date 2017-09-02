package com.toptenusers;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class UserMapper extends Mapper<LongWritable, Text,Text,IntWritable>{
	
	@Override
	public void map(LongWritable key, Text value, Context context){
		try {
			String line = value.toString();
			String[] key_val = line.split(",");
			String[] user_count = key_val[4].split("\t");
			System.out.println("User:::"+user_count[0]);
			context.write(new Text(user_count[0]), new IntWritable(1));
		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

}
