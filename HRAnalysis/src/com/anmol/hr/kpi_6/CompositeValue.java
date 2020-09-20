package com.anmol.hr.kpi_6;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;

public class CompositeValue implements Writable {
	private IntWritable promotionLast;
	private IntWritable leftCompany;
	
	public CompositeValue() {
		set(new IntWritable(), new IntWritable());
	}
	
	public CompositeValue(int promotionLast, int leftCompany) {
		set(new IntWritable(promotionLast), new IntWritable(leftCompany));
	}
	
	public CompositeValue( IntWritable promotionLast, IntWritable leftCompany) {
		set(promotionLast, leftCompany);
	}
	
	private void set(IntWritable promotionLast, IntWritable leftCompany) {
		this.promotionLast = promotionLast;
		this.leftCompany = leftCompany;
	}
	
	public IntWritable getPromotionLast() {
		return promotionLast;
	}
	
	public IntWritable getLeftCompany() {
		return leftCompany;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		promotionLast.readFields(in);
		leftCompany.readFields(in);
	}
	
	@Override
	public void write(DataOutput out) throws IOException {
		promotionLast.write(out);
		leftCompany.write(out);
	}
	
}
