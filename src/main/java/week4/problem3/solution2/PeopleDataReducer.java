package week4.problem3.solution2;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
public class PeopleDataReducer extends Reducer<Text, Text, Text, Text> {
    private static final String TAG_SEPARATOR = "~";
    private static final String DATA_SEPARATOR = ",";
    private static final String NEW_LINE = "\n";

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        context.write(null,new Text("profession,id,firstname,lastname,email,city,Field Name,salary"));
    }

    @Override
    protected void reduce(Text key, Iterable<Text> values,
                          Context context) throws IOException, InterruptedException {
        String value = "";
        String[] splittedValues = {};
        String tag = "";
        String salaryDetails = null, userDetails = null;
        StringBuilder data = new StringBuilder();
        for (Text text : values){
            value = text.toString();
            splittedValues = value.split(TAG_SEPARATOR);
            tag = splittedValues[0];

            if (tag.equalsIgnoreCase("SD")) {
                salaryDetails = splittedValues[1];
                break;
            }

        }
        for (Text txtValue : values) {
            data.append(key+DATA_SEPARATOR);
            value = txtValue.toString();
            splittedValues = value.split(TAG_SEPARATOR);
            tag = splittedValues[0];
            if (tag.equalsIgnoreCase("PD")) {
                userDetails = splittedValues[1];
                if (userDetails != null && salaryDetails != null) {
                    data.append(userDetails + DATA_SEPARATOR + salaryDetails);
                    data.append(NEW_LINE);
                }
            }

        }
        if(data.length()>0) data.setLength(data.length()-1);
        context.write(null, new Text(data.toString()));
    }
}
