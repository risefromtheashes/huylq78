package week4.problem3.solution3;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.ToolRunner;
import java.io.IOException;

public class Driver extends org.apache.hadoop.conf.Configured implements org.apache.hadoop.util.Tool {
    public static class JoinGroupingComparator extends WritableComparator {
        public JoinGroupingComparator() {
            super (PeopleProfessionKey.class, true);
        }

        @Override
        public int compare (WritableComparable a, WritableComparable b){
            PeopleProfessionKey first = (PeopleProfessionKey) a;
            PeopleProfessionKey second = (PeopleProfessionKey) b;

            return first.profession.compareTo(second.profession);
        }
    }

    public static class JoinSortingComparator extends WritableComparator {
        public JoinSortingComparator()
        {
            super (PeopleProfessionKey.class, true);
        }

        @Override
        public int compare (WritableComparable a, WritableComparable b){
            PeopleProfessionKey first = (PeopleProfessionKey) a;
            PeopleProfessionKey second = (PeopleProfessionKey) b;

            return first.compareTo(second);
        }
    }

    public static class SalaryMapper extends Mapper<LongWritable,
                Text, PeopleProfessionKey, JoinGenericWritable> {
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            String[] recordFields = value.toString().split(",");
            if(!recordFields[0].equalsIgnoreCase("profession")){
                String profession = recordFields[0];
                Long salary = Long.parseLong(recordFields[1]);


                PeopleProfessionKey recordKey = new PeopleProfessionKey(new Text(profession), PeopleProfessionKey.SALARY_RECORD);
                SalaryRecord record = new SalaryRecord(new LongWritable(salary));

                JoinGenericWritable genericRecord = new JoinGenericWritable(record);
                context.write(recordKey, genericRecord);
            }
        }
    }

    public static class PeopleMapper extends Mapper<LongWritable ,
            Text, PeopleProfessionKey, JoinGenericWritable>{
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String[] recordFields = value.toString().split(",");
            if(!recordFields[0].equalsIgnoreCase("id")){
                Text profession = new Text(recordFields[5]);
                LongWritable id = new LongWritable(Long.parseLong(recordFields[0]));
                Text firstName = new Text(recordFields[1]);
                Text lastName = new Text(recordFields[2]);
                Text email = new Text(recordFields[3]);
                Text city = new Text(recordFields[4]);
                Text fieldName = new Text(recordFields[6]);

                PeopleProfessionKey recordKey = new PeopleProfessionKey(profession, PeopleProfessionKey.PEOPLE_RECORD);
                PeopleRecord record = new PeopleRecord(id, firstName, lastName, email, city, fieldName);
                JoinGenericWritable genericRecord = new JoinGenericWritable(record);
                context.write(recordKey, genericRecord);
            }
        }
    }

    public static class JoinReducer extends Reducer<PeopleProfessionKey,
                JoinGenericWritable, NullWritable, Text> {
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            context.write(null, new Text("profession,id,firstname,lastname,email,city,Field Name,salary"));
        }

        public void reduce(PeopleProfessionKey key, Iterable<JoinGenericWritable> values,
                           Context context) throws IOException, InterruptedException{
            StringBuilder output = new StringBuilder();
            SalaryRecord salaryRecord = new SalaryRecord();
            PeopleRecord peopleRecord;
            for (JoinGenericWritable v : values) {
                if (key.recordType.equals(PeopleProfessionKey.SALARY_RECORD)){
                    Writable record = v.get();
                    salaryRecord = (SalaryRecord) record;
                    break;
                }
            }
            for (JoinGenericWritable v : values) {
                Writable record = v.get();
                if (key.recordType.equals(PeopleProfessionKey.PEOPLE_RECORD)) {
                    peopleRecord = (PeopleRecord) record;
                    output.append(key.profession.toString()).append(",");
                    output.append(peopleRecord.id.toString()).append(",");
                    output.append(peopleRecord.firstName.toString()).append(",");
                    output.append(peopleRecord.lastName.toString()).append(",");
                    output.append(peopleRecord.email.toString()).append(",");
                    output.append(peopleRecord.city.toString()).append(",");
                    output.append(peopleRecord.fieldName.toString()).append(",");
                    output.append(salaryRecord.salary.toString()).append("\n");
                }
            }
            output.setLength(output.length()-1);
                context.write(NullWritable.get(), new Text(output.toString()));
        }
    }

    public int run(String[] allArgs) throws Exception {
        String[] args = new GenericOptionsParser(getConf(), allArgs).getRemainingArgs();

        Job job = Job.getInstance(getConf());
        job.setJarByClass(Driver.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setMapOutputKeyClass(PeopleProfessionKey.class);
        job.setMapOutputValueClass(JoinGenericWritable.class);

        MultipleInputs.addInputPath(job, new Path(args[0]),
                TextInputFormat.class, PeopleMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]),
                TextInputFormat.class, SalaryMapper.class);

        job.setReducerClass(JoinReducer.class);

        job.setSortComparatorClass(JoinSortingComparator.class);
        job.setGroupingComparatorClass(JoinGroupingComparator.class);

        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        FileOutputFormat.setOutputPath(job, new Path(allArgs[2]));
        boolean status = job.waitForCompletion(true);
        if (status) {
            return 0;
        } else {
            return 1;
        }
    }

    public static void main(String[] args) throws Exception{
        Configuration conf = new Configuration();
        int res = ToolRunner.run(new Driver(), args);
    }
}
