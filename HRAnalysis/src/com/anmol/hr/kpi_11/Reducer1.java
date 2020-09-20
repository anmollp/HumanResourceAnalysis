package com.anmol.hr.kpi_11;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Reducer1 extends Reducer<IntWritable, LongWritable, Text, LongWritable>{
	
	public void reduce(IntWritable key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
		long experienceCount = 0;
		for(LongWritable value: values) {
			experienceCount += value.get();
		}
		context.write(new Text(key.get() + " years experience"), new LongWritable(experienceCount));
	}
}
