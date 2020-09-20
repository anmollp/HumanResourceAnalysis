package com.anmol.hr.kpi_13;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Reducer1 extends Reducer<DepartmentExperienceKey, LongWritable, Text, LongWritable>{

	public void reduce(DepartmentExperienceKey key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
		long totalCount = 0;
		for(LongWritable value: values) {
			totalCount+= value.get();
		}
		context.write(new Text(key.toString()), new LongWritable(totalCount));
	}
}
