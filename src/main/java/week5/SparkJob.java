package week5;

import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.catalyst.expressions.Sequence;
import org.apache.spark.sql.functions.*;
import java.util.List;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.collect_set;

public class SparkJob {
    public static void main(String[] args) {

        SparkSession spark = SparkSession
                .builder()
                .appName("Java Spark SQL basic example")
                .config("spark.some.config.option", "some-value")
//                .master("local[*]")
                .getOrCreate();
        Dataset<Row> df = spark.read().parquet("/home/quanghuy/KafkaProject/huylq78/Sample_data");
        df.createOrReplaceTempView("transaction");
        df = spark.sql("select * from transaction where device_model is not null");
        df.createOrReplaceTempView("transaction1");
        Dataset<Row> device_model_num_user = spark.sql("SELECT device_model, count(user_id) as count FROM transaction GROUP BY device_model");
        Dataset<Row> device_model_list_user = df.filter("user_id <> ''").groupBy("device_model").agg(collect_set("user_id"));
        device_model_num_user.write().mode(SaveMode.Overwrite).option("compression", "snappy").parquet("hdfs://10.140.0.5:9000/user/huylq78/device_model_num_user");
    }
}
