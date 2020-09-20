package com.anmol.hr.kpi_5;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Mapper1 extends Mapper<LongWritable, Text, DepartmentSalaryKey, LongWritable>{
	
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		try {
			if(!(key.get() == 0 && value.toString().contains("satisfaction_level"))) {
				String[] values = value.toString().split(",");
				String department = values[8];
				String salaryType = values[9];
				context.write(new DepartmentSalaryKey(department, salaryType), new LongWritable(1));
			}
		}
		catch(Exception e) {
			e.printStackTrace();
		}
	}

}
