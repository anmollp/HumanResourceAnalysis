package com.anmol.hr.kpi_14;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Driver {
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		String[] pathArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
	    if (pathArgs.length < 2)
	    {
	      System.err.println("HR Analysis KPI 14 Usage: HR Analysis <input-path> [...] <output-path>");
	      System.exit(2);
	    }
	    
		Job exp = Job.getInstance(conf, "HR Analysis KPI 14");
		
		exp.setJarByClass(Driver.class);
		exp.setMapperClass(Mapper1.class);
		exp.setReducerClass(Reducer1.class);
		
		exp.setMapOutputKeyClass(SalaryExperienceKey.class);
		exp.setMapOutputValueClass(LongWritable.class);
		exp.setOutputKeyClass(Text.class);
		exp.setOutputValueClass(LongWritable.class);
		
		
		FileInputFormat.addInputPath(exp, new Path(pathArgs[0]));
	    FileOutputFormat.setOutputPath(exp, new Path(pathArgs[1]));
	    
	    System.exit(exp.waitForCompletion(true) ? 0 : 1);
	}
}
