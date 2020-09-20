package com.anmol.hr.kpi_8;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;

public class CompositeValue implements Writable {
	private FloatWritable satisfactionLevel;
	private IntWritable monthlyHours;
	private IntWritable leftCompany;
	
	public CompositeValue() {
		set(new FloatWritable(), new IntWritable(), new IntWritable());
	}
	
	public CompositeValue(Float satisfactionLevel, int monthlyHours, int leftCompany) {
		set(new FloatWritable(satisfactionLevel), new IntWritable(monthlyHours), new IntWritable(leftCompany));
	}
	
	public CompositeValue(FloatWritable satisfactionLevel, IntWritable monthlyHours, IntWritable leftCompany) {
		set(satisfactionLevel, monthlyHours, leftCompany);
	}
	
	private void set(FloatWritable satisfactionLevel, IntWritable monthlyHours, IntWritable leftCompany) {
		this.satisfactionLevel = satisfactionLevel;
		this.monthlyHours = monthlyHours;
		this.leftCompany = leftCompany;
	}
	
	public FloatWritable getSatisfactionLevel() {
		return satisfactionLevel;
	}
	
	public IntWritable getMonthlyHours() {
		return monthlyHours;
	}
	
	public IntWritable getLeftCompany() {
		return leftCompany;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		satisfactionLevel.readFields(in);
		monthlyHours.readFields(in);
		leftCompany.readFields(in);
	}
	
	@Override
	public void write(DataOutput out) throws IOException {
		satisfactionLevel.write(out);
		monthlyHours.write(out);
		leftCompany.write(out);
	}
	
}
