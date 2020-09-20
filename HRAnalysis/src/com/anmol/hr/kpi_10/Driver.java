package com.anmol.hr.kpi_10;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Driver {
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		String[] pathArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
	    if (pathArgs.length < 4)
	    {
	      System.err.println("HR Analysis KPI 10 Usage: Driver <input-path> [...] <output-path>");
	      System.exit(2);
	    }
	    
	    Job hra = Job.getInstance(conf, "HR Analysis KPI 10 part 1");
	    hra.setJarByClass(Driver.class);
	    hra.setMapperClass(Mapper1.class);
	    hra.setReducerClass(Reducer1.class);
	    
	    hra.setMapOutputKeyClass(Text.class);
	    hra.setMapOutputValueClass(DoubleWritable.class);
	    hra.setOutputKeyClass(Text.class);
	    hra.setOutputValueClass(DoubleWritable.class);
	    
	    FileInputFormat.addInputPath(hra, new Path(pathArgs[0]));
	    FileOutputFormat.setOutputPath(hra, new Path(pathArgs[1]));
	    
	    hra.waitForCompletion(true);
	    
	    Job hra2 = Job.getInstance(conf, "HR Analysis KPI 10 part 2");
	    hra2.setJarByClass(Driver.class);
	    hra2.setMapperClass(Mapper2.class);
	    hra2.setReducerClass(Reducer1.class);
	    
	    hra2.setMapOutputKeyClass(Text.class);
	    hra2.setMapOutputValueClass(DoubleWritable.class);
	    hra2.setOutputKeyClass(Text.class);
	    hra2.setOutputValueClass(DoubleWritable.class);
	    
	    FileInputFormat.addInputPath(hra2, new Path(pathArgs[0]));
	    FileOutputFormat.setOutputPath(hra2, new Path(pathArgs[2]));
	    
	    hra2.waitForCompletion(true);
	    
	    Job hra3 = Job.getInstance(conf, "HR Analysis KPI 10 part 3");
	    hra3.setJarByClass(Driver.class);
	    hra3.setMapperClass(Mapper3.class);
	    hra3.setReducerClass(Reducer3.class);
	    
	    hra3.setMapOutputKeyClass(DepartmentSalaryKey.class);
	    hra3.setMapOutputValueClass(LongWritable.class);
	    hra3.setOutputKeyClass(DepartmentSalaryKey.class);
	    hra3.setOutputValueClass(FloatWritable.class);
	    
	    FileInputFormat.addInputPath(hra3, new Path(pathArgs[0]));
	    FileOutputFormat.setOutputPath(hra3, new Path(pathArgs[3]));
	    
	    System.exit(hra3.waitForCompletion(true) ? 0 : 1);
	}
}
