package com.anmol.hr.kpi_5;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class DepartmentSalaryKey implements WritableComparable<DepartmentSalaryKey>{

	private Text department;
	private Text salaryType;
	
	public DepartmentSalaryKey() {
		set(new Text(), new Text());
	}
	
	public DepartmentSalaryKey(Text department, Text salaryType) {
		set(department, salaryType);
	}
	
	public DepartmentSalaryKey(String department, String salaryType) {
		set(new Text(department), new Text(salaryType));
	}
	
	private void set(Text department, Text salaryType) {
		this.department = department;
		this.salaryType = salaryType;
	}
	
	public Text getDepartment() {
		return department;
	}
	
	public Text getSalaryType() {
		return salaryType;
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		department.readFields(in);
		salaryType.readFields(in);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		department.write(out);
		salaryType.write(out);
	}

	@Override
	public int compareTo(DepartmentSalaryKey o) {
		if(this.department.toString().equals(o.department.toString())) {
			return salaryType.toString().compareTo(o.salaryType.toString());
		}
		else {
			return department.toString().compareTo(o.department.toString());
		}
	}
	
	@Override
	public int hashCode()
	{
		final int prime = 31;
	    int result = 1;
	    result = prime * result + ((department == null) ? 0 : department.toString().hashCode());
	    result = prime * result + ((salaryType == null) ? 0 : salaryType.toString().hashCode());
	    return result;
	}
	
	@Override
	public String toString() {
		return department.toString() + "-" + salaryType.toString();
		}
}
