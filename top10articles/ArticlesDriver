package com.toptenarticles;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class ArticlesDriver {
	
	public static void main(String[] args){
	Configuration conf = new Configuration();
	try {
		Job job = new Job(conf,"Top 10 Articles");
		job.setJarByClass(ArticlesDriver.class);
		job.setMapperClass(ArticlesMapper.class);
		job.setReducerClass(ArticlesReducer.class);
		
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
