package com.anmol.hr.kpi_9;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Reducer1 extends Reducer<Text, CompositeValue, Text, Text>{
	
	public void reduce(Text key, Iterable<CompositeValue> values, Context context) throws IOException, InterruptedException {
		float satisfactionLevelSum = 0;
		float monthlyHoursSum = 0;
		long leftCompanySum = 0;
		long totalObservations = 0;
		for(CompositeValue value: values) {
			satisfactionLevelSum += value.getSatisfactionLevel().get();
			monthlyHoursSum += value.getMonthlyHours().get();
			if(value.getLeftCompany().get() == 1) {
				leftCompanySum += 1;
			}
			totalObservations++;
		}
		float averageSatisfactionLevel = satisfactionLevelSum/totalObservations;
		float averagemonthlyHours = monthlyHoursSum/totalObservations;
		context.write(key, new Text("AvgSatLev: " + averageSatisfactionLevel + "\t" + "AvgMonHrs: " + averagemonthlyHours +
				"\t" + "#PplLeft: " + leftCompanySum));
	}

}
