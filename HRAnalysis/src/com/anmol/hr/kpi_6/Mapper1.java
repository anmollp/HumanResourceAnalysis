package com.anmol.hr.kpi_6;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Mapper1 extends Mapper<LongWritable, Text, Text, CompositeValue>{
	
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		if(!(key.get() == 0 && value.toString().contains("satisfaction_level"))) {
		String[] values = value.toString().split(",");
		String department = values[8];
		int lastPromotion = Integer.parseInt(values[7]);
		int leftCompany = Integer.parseInt(values[6]);
		context.write(new Text(department), new CompositeValue(lastPromotion, leftCompany));
		}
	}
}
