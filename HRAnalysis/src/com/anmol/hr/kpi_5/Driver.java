package com.anmol.hr.kpi_5;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
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
	    if (pathArgs.length < 2)
	    {
	      System.err.println("HR Analysis KPI 5 Usage: HR Analysis <input-path> [...] <output-path>");
	      System.exit(2);
	    }


		Job job = Job.getInstance(conf, "HR Analysis KPI 5");
		
		job.setJarByClass(Driver.class);
		job.setMapperClass(Mapper1.class);
		job.setCombinerClass(Combiner1.class);
		job.setReducerClass(Reducer1.class);
		job.setNumReduceTasks(1);
		
		job.setMapOutputKeyClass(DepartmentSalaryKey.class);
		job.setMapOutputValueClass(LongWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job, new Path(pathArgs[0]));
	    FileOutputFormat.setOutputPath(job, new Path(pathArgs[1]));
	    
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
