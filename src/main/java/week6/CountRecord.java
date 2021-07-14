package week6;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.util.LongAccumulator;

public class CountRecord {
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .appName("SparkKafka")
                .master("local")
                .getOrCreate();
        spark.sparkContext().setLogLevel("ERROR");
        JavaRDD rdd = spark.read().parquet("hdfs://10.140.0.5:9000/user/huylq78/data_tracking").toJavaRDD();
        final LongAccumulator accumulator = spark.sparkContext().longAccumulator("count");
        rdd.foreach(
                x -> {accumulator.add(1);
                    System.out.println(accumulator);
                }
        );
    }
}