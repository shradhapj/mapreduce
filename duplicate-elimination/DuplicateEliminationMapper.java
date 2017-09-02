package com.duplicate.elimination;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class DuplicateEliminationMapper extends Mapper<LongWritable,Text,Text,IntWritable>{
	
	public void map(LongWritable key, Text value, Context context ) throws
	IOException, InterruptedException {
	//	String txnString = value.toString();	
	//	String[] txnData = txnString.split( "," );
	//	String user_name =  txnData[4];
		context.write( value, new IntWritable(1));
		System.out.println("Key :"+key+" \t"+"Value: "+value);
	}

}
