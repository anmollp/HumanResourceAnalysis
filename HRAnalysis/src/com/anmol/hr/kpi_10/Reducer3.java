package com.anmol.hr.kpi_10;

import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class Reducer3 extends Reducer<DepartmentSalaryKey, LongWritable, DepartmentSalaryKey, FloatWritable>{
	
	public void reduce(DepartmentSalaryKey key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
		float numberOfObservations = 0;
		float numberOfFolksLeft = 0;
		for(LongWritable value: values) {
			if(value.get() == 1) {
				numberOfFolksLeft++;
			}
			numberOfObservations++;
		}
		context.write(key, new FloatWritable(((numberOfFolksLeft/numberOfObservations) * 100)));
	}
}
