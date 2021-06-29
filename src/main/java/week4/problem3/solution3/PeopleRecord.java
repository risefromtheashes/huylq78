package week4.problem3.solution3;

import org.apache.hadoop.io.*;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class PeopleRecord implements Writable {
    public LongWritable id = new LongWritable();
    public Text firstName = new Text();
    public Text lastName = new Text();
    public Text email = new Text();
    public Text city = new Text();
    public Text fieldName = new Text();


    public PeopleRecord(){}

    public PeopleRecord(LongWritable id, Text firstName, Text lastName, Text email, Text city, Text fieldName) {
        this.id = id;
        this.firstName = firstName;
        this.lastName = lastName;
        this.email = email;
        this.city = city;
        this.fieldName = fieldName;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        this.id.write(dataOutput);
        this.firstName.write(dataOutput);
        this.lastName.write(dataOutput);
        this.email.write(dataOutput);
        this.city.write(dataOutput);
        this.fieldName.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.id.readFields(dataInput);
        this.firstName.readFields(dataInput);
        this.lastName.readFields(dataInput);
        this.email.readFields(dataInput);
        this.city.readFields(dataInput);
        this.fieldName.readFields(dataInput);
    }
}

