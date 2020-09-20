package com.anmol.hr.kpi_12;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Mapper1 extends Mapper<LongWritable, Text, Text, LongWritable>{
	
	
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		try {
			if(!(key.get() == 0 && value.toString().contains("satisfaction_level"))) {
				String[] values = value.toString().split(",");
				int left = Integer.parseInt(values[6]);
				String department = values[8];
				context.write(new Text(department), new LongWritable(left));
			}
		}
		catch(Exception e) {
			e.printStackTrace();
		}
	}
}
