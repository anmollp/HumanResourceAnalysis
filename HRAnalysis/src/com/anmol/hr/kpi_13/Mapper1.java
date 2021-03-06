package com.anmol.hr.kpi_13;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Mapper1 extends Mapper<LongWritable, Text, DepartmentExperienceKey, LongWritable>{
	
	
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		try {
			if(!(key.get() == 0 && value.toString().contains("satisfaction_level"))) {
				String[] values = value.toString().split(",");
				int yearsOfExperience = Integer.parseInt(values[4]);
				String department = values[8];
				if (yearsOfExperience >= 8) {
					context.write(new DepartmentExperienceKey(department, yearsOfExperience), new LongWritable(1));
				}
			}
		}
		catch(Exception e) {
			e.printStackTrace();
		}
	}
}
