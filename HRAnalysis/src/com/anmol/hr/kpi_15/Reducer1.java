package com.anmol.hr.kpi_15;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Reducer1 extends Reducer<Text, LongWritable, Text, FloatWritable>{
	float totalProjectsCount = 0;
	Map<String, Long> DepartmentProjects = new HashMap<>();
	public void reduce(Text key, Iterable<LongWritable> values, Context context) {
		long deptProjects = 0;
		for(LongWritable value: values) {
			deptProjects += value.get();
			totalProjectsCount += value.get();
		}
		DepartmentProjects.put(key.toString(), deptProjects);
	}
	
	public void cleanup(Context context) throws IOException, InterruptedException {
		for(Map.Entry<String, Long> entry : DepartmentProjects.entrySet()) {
			float percent40 = (entry.getValue()/totalProjectsCount) * 100; 
			if (percent40 > 40) {
			context.write(new Text(entry.getKey()), new FloatWritable(percent40));
			}
		}
	}
}
