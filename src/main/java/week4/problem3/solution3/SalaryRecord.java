package week4.problem3.solution3;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.*;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class SalaryRecord implements Writable {
    public LongWritable salary = new LongWritable();

    public SalaryRecord() {
    }

    public SalaryRecord(LongWritable salary) {
        this.salary = salary;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        this.salary.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.salary.readFields(dataInput);
    }
}
