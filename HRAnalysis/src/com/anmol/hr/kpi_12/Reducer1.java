package com.anmol.hr.kpi_12;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Reducer1 extends Reducer<Text, LongWritable, Text, NullWritable>{

	public void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
		long leftCount = 0;
		long totalCount = 0;
		for(LongWritable value: values) {
			if(value.get() == 1) {
				leftCount += value.get();
			}
			totalCount++;
		}
		if (((leftCount/totalCount) * 100 > 70)) {
			context.write(key, NullWritable.get());
		}
	}
}
