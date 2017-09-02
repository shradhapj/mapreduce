package com.classify.users;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;



public class ClassifyUserDriver {
	public static void main(String args[]){
		Configuration conf = new Configuration();
		try {
			Job job = new Job(conf,"Classification");
			job.setJarByClass(ClassifyUserDriver.class);
			job.setMapperClass(ClassifyUserMapper.class);
			job.setReducerClass(ClassifyUserReducer.class);
			job.setPartitionerClass(UserPartitioner.class);
			job.setNumReduceTasks(3);
			
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(IntWritable.class);
			
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(LongWritable.class);
			
			 FileInputFormat.addInputPath( job, new Path( args[0] ) );
			 FileOutputFormat.setOutputPath( job, new Path( args[1] ) );
			 System.exit( job.waitForCompletion( true ) ? 0 : 1 );
			
		} catch (IOException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
