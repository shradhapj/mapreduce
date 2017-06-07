package com.classify.users;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class ClassifyUserMapper extends Mapper<LongWritable,Text,Text,IntWritable>{
	
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		String[] tokens = value.toString().split(",");

		String ip_address = tokens[4].toString();
		int change_size = Integer.parseInt(tokens[1]);
		String username = "";
		if(isIpAddress(ip_address))
			username = "Unregistered";
		else
			username = "Registered";
		
		context.write(new Text(username), new IntWritable(change_size));
	}//end of map

	private boolean isIpAddress(String ip_address) {
		Matcher m1 = ClassifyUserMapper.VALID_IPV4_PATTERN.matcher(ip_address);
	    if (m1.matches()) {
	      return true;
	    }
	    Matcher m2 = ClassifyUserMapper.VALID_IPV6_PATTERN.matcher(ip_address);
	    return m2.matches();
	  }
	private static Pattern VALID_IPV4_PATTERN = null;
	  private static Pattern VALID_IPV6_PATTERN = null;
	  private static final String ipv4Pattern = "(([01]?\\d\\d?|2[0-4]\\d|25[0-5])\\.){3}([01]?\\d\\d?|2[0-4]\\d|25[0-5])";
	  private static final String ipv6Pattern = "([0-9a-f]{1,4}:){7}([0-9a-f]){1,4}";

	  static {
	    try {
	      VALID_IPV4_PATTERN = Pattern.compile(ipv4Pattern, Pattern.CASE_INSENSITIVE);
	      VALID_IPV6_PATTERN = Pattern.compile(ipv6Pattern, Pattern.CASE_INSENSITIVE);
	    } catch (PatternSyntaxException e) {
	    }
	  }
	}


