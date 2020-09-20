package com.anmol.hr.kpi_10;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Mapper2 extends Mapper<LongWritable, Text, Text, DoubleWritable>{
	
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		try {
			if(!(key.get() == 0 && value.toString().contains("satisfaction_level"))) {
				String[] values = value.toString().split(",");
				String department = values[8];
				Double evaluation = Double.parseDouble(values[1]);
				context.write(new Text(department), new DoubleWritable(evaluation));
			}
		}
		catch(Exception e) {
			e.printStackTrace();
		}
	}

}
