package week5;

import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.catalyst.expressions.Sequence;
import org.apache.spark.sql.functions.*;
import java.util.List;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.collect_set;

public class Spark {
    public static void main(String[] args) {

        SparkSession spark = SparkSession
                .builder()
                .appName("Java Spark SQL basic example")
                .config("spark.some.config.option", "some-value")
//                .master("local[*]")
                .getOrCreate();
        Dataset<Row> df = spark.read().parquet("/Sample_data");
        df.createOrReplaceTempView("transaction");
        spark.sql("select * from transaction where device_model is not null").createOrReplaceTempView("transaction1");
        Dataset<Row> device_model_num_user = spark.sql("SELECT device_model, count(user_id) as count FROM transaction1 GROUP BY device_model");
        Dataset<Row> device_model_list_user = df.filter("user_id <> ''").groupBy("device_model").agg(collect_set("user_id")).withColumnRenamed("collect_set(user_id)", "list_user_id");
        spark.sql("select concat(user_id,' ',device_model) as user_id_device_model, button_id from transaction where button_id is not null").createOrReplaceTempView("tmp");
        Dataset<Row> button_count_by_user_id_device_model = spark.sql("select user_id_device_model, button_id, count(*) as count from tmp group by user_id_device_model, button_id");
//        device_model_num_user.show();
//        device_model_list_user.show();
//        button_count_by_user_id_device_model.show();
        device_model_num_user.repartition(1).write().mode(SaveMode.Overwrite).option("compression", "snappy").parquet("hdfs://10.140.0.5:9000/user/huylq78/device_model_num_user");
        device_model_list_user.repartition(1).write().mode(SaveMode.Overwrite).orc("hdfs://10.140.0.5:9000/user/huylq78/device_model_list_user");
        button_count_by_user_id_device_model.repartition(1).write().mode(SaveMode.Overwrite).parquet("hdfs://10.140.0.5:9000/user/huylq78/button_count_by_user_id_device_model");
    }
}
