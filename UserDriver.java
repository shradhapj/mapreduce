package com.toptenusers;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;



public class UserDriver {
	public static void main(String [] args){
		Configuration conf = new Configuration();
		try {
			conf.set("mapred.textoutputformat.separator", ",");
			Job job = new Job(conf, "Top10Users");
			job.setJarByClass(UserDriver.class);
			job.setMapperClass(UserMapper.class);
			job.setReducerClass(UserReducer.class);
			
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(IntWritable.class);
			
			
			
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(IntWritable.class);
			
			 FileInputFormat.addInputPath( job, new Path( args[0] ) );
			    FileOutputFormat.setOutputPath( job, new Path( args[1] ) );
			    
			    try {
					System.exit( job.waitForCompletion( true ) ? 0 : 1 );
				} catch (ClassNotFoundException | InterruptedException e) {
					e.printStackTrace();
				}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

}
