package com.anmol.hr.kpi_1;

import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Reducer1 extends Reducer<Text, FloatWritable, Text, FloatWritable>
{
	public void reduce(Text key, Iterable<FloatWritable> values, Context context) throws IOException, InterruptedException {
		float satisfactionLevelSum = 0;
		long totalCount = 0;
		for(FloatWritable value : values) {
			satisfactionLevelSum += value.get();
			totalCount += 1;
			}
		context.write(key, new FloatWritable(satisfactionLevelSum/totalCount));
	}
}