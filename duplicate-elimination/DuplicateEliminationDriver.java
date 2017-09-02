package com.duplicate.elimination;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;



public class DuplicateEliminationDriver {
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = new Job(conf, "Transactions");
		//Ignore the warning as new techniques for getting handle to Job is

		job.setJarByClass( DuplicateEliminationDriver.class );
		job.setMapperClass( DuplicateEliminationMapper.class );
		//job.setCombinerClass(MaxReducer.class);
		job.setReducerClass( DuplicationEliminationReducer.class );
		job.setMapOutputKeyClass( Text.class );
		job.setMapOutputValueClass( IntWritable.class );
		job.setOutputKeyClass( Text.class );
		job.setOutputValueClass( IntWritable.class );
		FileInputFormat.addInputPath( job, new Path( args[0] ) );
		FileOutputFormat.setOutputPath( job, new Path( args[1] ) );
		System.exit( job.waitForCompletion( true ) ? 0 : 1 );
		}


}
