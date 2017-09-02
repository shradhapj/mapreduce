package com.changes;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ChangesMapper extends Mapper<LongWritable,Text,Text,Text>{
	
	@Override
	public void map(LongWritable key, Text value, Context context){
		String line = value.toString();
		String [] tokens = line.split(",");
		String edit="";
		
		if(tokens[1].charAt(0)=='-')
			edit = "deleted";
		else if(tokens[1].charAt(0)=='0')
			edit = "added";
		else
			edit = "added";
		Integer change_size_int = Math.abs(Integer.parseInt(tokens[1]));
		String change_size = change_size_int.toString();
		try {
			context.write(new Text(edit), new Text(change_size));
		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

}
