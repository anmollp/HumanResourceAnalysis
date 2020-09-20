package com.anmol.hr.kpi_10;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Mapper3 extends Mapper<LongWritable, Text, DepartmentSalaryKey, LongWritable>{
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		try {
			if(!(key.get() == 0 && value.toString().contains("satisfaction_level"))) {
				String[] values = value.toString().split(",");
				String dept = values[8];
				String salaryType = values[9];
				long left = Long.parseLong(values[6].toString());
				context.write(new DepartmentSalaryKey(dept, salaryType), new LongWritable(left));
			}
		}
		catch(Exception e) {
			e.printStackTrace();
		}
	}
}
