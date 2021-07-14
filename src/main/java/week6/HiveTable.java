package week6;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class HiveTable {
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .appName("SparkKafka")
                .master("local")
                .getOrCreate();
        spark.sparkContext().setLogLevel("ERROR");
        Dataset<Row> df = spark.read().parquet("hdfs://10.140.0.5:9000/user/huylq78/data_tracking");
        df.createOrReplaceTempView("tmp");
        spark.sql("create table data_tracking as select * from tmp");
    }
}
