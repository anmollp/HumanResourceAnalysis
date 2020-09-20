package com.anmol.hr.kpi_8;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Mapper1 extends Mapper<LongWritable, Text, Text, CompositeValue>{
	
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		if(!(key.get() == 0 && value.toString().contains("satisfaction_level"))) {
		String[] values = value.toString().split(",");
		String department = values[8];
		float satisfactionLevel = Float.parseFloat(values[0]);
		int monthlyHours = Integer.parseInt(values[3]);
		int leftCompany = Integer.parseInt(values[6]);
		if(values[9].equals("low")) {
			context.write(new Text(department), new CompositeValue(satisfactionLevel, monthlyHours, leftCompany));
			}
		}
	}
}
