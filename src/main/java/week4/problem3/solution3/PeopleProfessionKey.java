package week4.problem3.solution3;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class PeopleProfessionKey implements WritableComparable<PeopleProfessionKey> {
    public Text profession = new Text();
    public IntWritable recordType = new IntWritable();
    public static final IntWritable SALARY_RECORD = new IntWritable(0);
    public static final IntWritable PEOPLE_RECORD = new IntWritable(1);
    public PeopleProfessionKey() {
    }

    public PeopleProfessionKey(Text profession, IntWritable recordType) {
        this.profession = profession;
        this.recordType = recordType;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        this.profession.write(dataOutput);
        this.recordType.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.profession.readFields(dataInput);
        this.recordType.readFields(dataInput);
    }
    @Override
    public int compareTo(PeopleProfessionKey other) {
        if (this.profession.equals(other.profession )) {
            return this.recordType.compareTo(other.recordType);
        } else {
            return this.profession.compareTo(other.profession);
        }
    }

    public boolean equals (PeopleProfessionKey other) {
        return this.profession.equals(other.profession) && this.recordType.equals(other.recordType );
    }

    public int hashCode() {
        return this.profession.hashCode();
    }


}
