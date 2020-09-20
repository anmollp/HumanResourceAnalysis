package com.anmol.hr.kpi_2;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
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
	      System.err.println("HR Analysis KPI 2 Usage: HR Analysis <input-path> [...] <output-path>");
	      System.exit(2);
	    }


		Job job = Job.getInstance(conf, "HR Analysis KPI 2");
		
		job.setJarByClass(Driver.class);
		job.setMapperClass(Mapper1.class);
		job.setReducerClass(Reducer1.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		FileInputFormat.addInputPath(job, new Path(pathArgs[0]));
	    FileOutputFormat.setOutputPath(job, new Path(pathArgs[1]));
	    
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
