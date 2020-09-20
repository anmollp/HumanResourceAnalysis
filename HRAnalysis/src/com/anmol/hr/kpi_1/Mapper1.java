package com.anmol.hr.kpi_1;

import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Mapper1 extends Mapper<LongWritable, Text, Text, FloatWritable>{
	
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		try {
			if(!(key.get() == 0 && value.toString().contains("satisfaction_level"))) {
			String[] values = value.toString().split(",");
			String department = values[8];
			float satisFactionLevel = Float.parseFloat(values[0]);
			context.write(new Text(department), new FloatWritable(satisFactionLevel));
				}
			}
		catch(Exception e) {
			e.printStackTrace();
		}
	}
}
