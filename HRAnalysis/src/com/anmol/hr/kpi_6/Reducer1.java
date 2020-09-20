package com.anmol.hr.kpi_6;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Reducer1 extends Reducer<Text, CompositeValue, Text, LongWritable>{
	
	public void reduce(Text key, Iterable<CompositeValue> values, Context context) throws IOException, InterruptedException {
		long leftCompanySum = 0;
		for(CompositeValue value: values) {
				leftCompanySum += value.getLeftCompany().get();
		}
		context.write(key, new LongWritable(leftCompanySum));
	}

}
