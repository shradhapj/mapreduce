package com.changes;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;



public class ChangesDriver {
	
	public static void main(String args[]){
		Configuration conf = new Configuration();
		try {
			Job job =  new Job(conf, "Changes") ;
			job.setJarByClass(ChangesDriver.class);
			job.setMapperClass(ChangesMapper.class);
			job.setReducerClass(ChangesReducer.class);
			
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(Text.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(LongWritable.class);
			
			FileInputFormat.addInputPath( job, new Path( args[0] ) );
		    FileOutputFormat.setOutputPath( job, new Path( args[1] ) );
		    
		    System.exit( job.waitForCompletion( true ) ? 0 : 1 );
		} catch (IOException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

}
