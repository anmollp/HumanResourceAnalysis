package com.anmol.hr.kpi_5;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class Combiner1 extends Reducer<DepartmentSalaryKey, LongWritable, DepartmentSalaryKey, LongWritable>{
	public void reduce(DepartmentSalaryKey key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
		long count = 0;
		for(LongWritable value: values) {
			count += value.get();
		}
		context.write(key, new LongWritable(count));
	}

}
