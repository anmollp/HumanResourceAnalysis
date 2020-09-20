package com.anmol.hr.kpi_13;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class DepartmentExperienceKey implements WritableComparable<DepartmentExperienceKey>{
	
	private Text department;
	private IntWritable yearsOfExperience;

	public DepartmentExperienceKey() {
		set(new Text(), new IntWritable());	
	}
	
	private void set(Text department, IntWritable yearsOfExperience) {
		this.department = department;
		this.yearsOfExperience = yearsOfExperience;
	}
	
	public DepartmentExperienceKey(String department, int yearsOfExperience) {
		set(new Text(department), new IntWritable(yearsOfExperience));
	}

	public DepartmentExperienceKey(Text department, IntWritable yearsOfExperience) {
		set(department, yearsOfExperience);
	}
	
	public IntWritable getyearsOfExperience() {
		return yearsOfExperience;
	}
	
	public Text getDepartment() {
		return department;
	}
	@Override
	public void readFields(DataInput in) throws IOException {
		department.readFields(in);
		yearsOfExperience.readFields(in);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		department.write(out);
		yearsOfExperience.write(out);
		
	}

	@Override
	public int compareTo(DepartmentExperienceKey o) {
		if(department.toString().equals(o.department.toString())) {
			return -1 * (yearsOfExperience.get() - o.yearsOfExperience.get());
		}
		else {
			return department.toString().compareTo(o.department.toString());
		}
	}
	
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
	    result = prime * result + ((department == null) ? 0 : department.toString().hashCode());
	    result = prime * result + ((yearsOfExperience == null) ? 0 : yearsOfExperience.toString().hashCode());
	    return result;
	}

	@Override
	public String toString() {
		return department.toString() + "-" + yearsOfExperience.toString() + " years of experience";
	}
}
