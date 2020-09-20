package com.anmol.hr.kpi_10;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Reducer1 extends Reducer<Text, DoubleWritable, Text, DoubleWritable>{
	public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
		int totalObservations = 0;
		double sumOfObservations = 0;
		for(DoubleWritable value: values) {
			totalObservations++;
			sumOfObservations += Double.parseDouble(value.toString());
		}
		context.write(key, new DoubleWritable(sumOfObservations/totalObservations));
	}
}
