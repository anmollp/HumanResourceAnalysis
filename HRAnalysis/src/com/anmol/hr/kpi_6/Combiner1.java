package com.anmol.hr.kpi_6;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Combiner1 extends Reducer<Text, CompositeValue, Text, CompositeValue>{
	public void reduce(Text key, Iterable<CompositeValue> values, Context context) throws IOException, InterruptedException {
		for(CompositeValue value: values) {
			if(value.getLeftCompany().get() == 1 && value.getPromotionLast().get() == 1) {
				context.write(key, new CompositeValue(value.getPromotionLast(), value.getLeftCompany()));
			}
		}
		
	}
}
