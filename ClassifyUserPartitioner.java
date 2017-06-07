package com.classify.users;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class UserPartitioner extends Partitioner<Text, IntWritable>{

	@Override
	public int getPartition(Text key, IntWritable value, int numReduceTasks) {
		String change_size = value.toString();
		int chage_Size_Int = Math.abs(Integer.parseInt(change_size));
		
		//this is done to avoid performing mod with 0
		if(numReduceTasks == 0)
			return 0;

		//if the age is <20, assign partition 0
		if(chage_Size_Int <=12){				
			return 0;
		}
		//else if the age is between 20 and 50, assign partition 1
		if(chage_Size_Int >12 && chage_Size_Int <=50){
			
			return 1;
		}
		//otherwise assign partition 2
		else
			return 2;
	} //end of partition function

	}


