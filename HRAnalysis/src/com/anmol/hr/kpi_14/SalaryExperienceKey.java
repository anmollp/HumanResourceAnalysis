package com.anmol.hr.kpi_14;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class SalaryExperienceKey implements WritableComparable<SalaryExperienceKey>{
	
	private Text salaryType;
	private IntWritable yearsOfExperience;

	public SalaryExperienceKey() {
		set(new Text(), new IntWritable());	
	}
	
	private void set(Text salaryType, IntWritable yearsOfExperience) {
		this.salaryType = salaryType;
		this.yearsOfExperience = yearsOfExperience;
	}
	
	public SalaryExperienceKey(String salaryType, int yearsOfExperience) {
		set(new Text(salaryType), new IntWritable(yearsOfExperience));
	}

	public SalaryExperienceKey(Text salaryType, IntWritable yearsOfExperience) {
		set(salaryType, yearsOfExperience);
	}
	
	public IntWritable getyearsOfExperience() {
		return yearsOfExperience;
	}
	
	public Text getSalaryType() {
		return salaryType;
	}
	@Override
	public void readFields(DataInput in) throws IOException {
		salaryType.readFields(in);
		yearsOfExperience.readFields(in);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		salaryType.write(out);
		yearsOfExperience.write(out);
		
	}

	@Override
	public int compareTo(SalaryExperienceKey o) {
		if(salaryType.toString().equals(o.salaryType.toString())) {
			return -1 * (yearsOfExperience.get() - o.yearsOfExperience.get());
		}
		else {
			return salaryType.toString().compareTo(o.salaryType.toString());
		}
	}
	
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
	    result = prime * result + ((salaryType == null) ? 0 : salaryType.toString().hashCode());
	    result = prime * result + ((yearsOfExperience == null) ? 0 : yearsOfExperience.toString().hashCode());
	    return result;
	}

	@Override
	public String toString() {
		return "Salary Type:" + salaryType.toString() + "\t" + yearsOfExperience.toString() + " years of experience ";
	}
}
