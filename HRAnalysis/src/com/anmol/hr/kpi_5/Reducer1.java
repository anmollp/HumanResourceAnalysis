package com.anmol.hr.kpi_5;

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Reducer1 extends Reducer<DepartmentSalaryKey, LongWritable, Text, Text>{
	private Map<String, Long> depSal;
	private float totalCount = 0;
	
	public void setup(Context context) {
		depSal = new TreeMap<>();
		totalCount = 0;
	}
	
	public void reduce(DepartmentSalaryKey key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
		for(LongWritable value: values) {
			depSal.put(key.toString(), value.get());
			totalCount += value.get();
		}
	}
	
	public void cleanup(Context context) throws IOException, InterruptedException {
		for(Map.Entry<String, Long> entry: depSal.entrySet()) {
			context.write(new Text(entry.getKey().toString()), new Text((entry.getValue()/totalCount)*100 + "%"));
		}
	}

}