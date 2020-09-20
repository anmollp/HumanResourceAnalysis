package com.anmol.hr.kpi_3;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Reducer1 extends Reducer<Text, IntWritable, Text, IntWritable>
{
	public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
		int workhoursCount = 0;
		for(IntWritable value: values) {
			workhoursCount += value.get();
		}
		context.write(key, new IntWritable(workhoursCount));
	}
}
