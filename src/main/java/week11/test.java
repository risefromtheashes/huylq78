package week11;

import io.confluent.kafka.schemaregistry.client.rest.RestService;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.json.JSONArray;
import org.json.JSONObject;
import week6.datatracking.Datatracking;

import java.util.concurrent.TimeoutException;

public class test {
    public static String parseJson(String json, String param){
        JSONObject obj = new JSONObject(json);
        if(obj.getJSONObject("payload").get(param) != null) return obj.getJSONObject("payload").get(param).toString();
        return "";
    }
    public static String parseTime(String json){
        JSONObject obj = new JSONObject(json);
        return obj.getJSONObject("payload").getJSONObject("source").get("ts_ms").toString();
    }
//    public static String parseValue(String json){
//        json = parseJson(json, "patch");
//        if(json != null){
//            json = json.substring(1, json.length()-1);
//            json = json.replaceAll("\\\\", "");
//            JSONObject obj = new JSONObject(json);
//            if(obj.getJSONObject("$set").get("name") != null) return obj.getJSONObject("$set").get("name").toString();
//        }
//        return "";
//    }
    public static void main(String[] args) throws TimeoutException, StreamingQueryException {
        SparkSession spark = SparkSession.builder()
                .appName("SparkKafka")
                .master("local")
                .getOrCreate();
        spark.sparkContext().setLogLevel("ERROR");
        Dataset<Row> df = spark.readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers","10.140.0.3:9092")
                .option("subscribe","test1.huylq78.movie")
                .option("group.id","group1")
                .option("startingOffsets", "earliest")
                .option("auto.offset.reset","true")
                .option("value.serializer","org.apache.kafka.common.serialization.ByteArrayDeserializer")
                .load();
//        Dataset<byte[]>words = df.select("key").as(Encoders.BINARY());
// result.join(tmp, expr)

        Dataset<String> record = df.selectExpr("CAST(value AS STRING)").as(Encoders.STRING()).map(
                (MapFunction<String, String>)
                        s-> (
                            parseJson(s, "filter") + "#" + parseJson(s,"patch") + "#" + parseTime(s)
                        ),Encoders.STRING()
        );

        Dataset<Row> result = record.withColumn("filter", functions.split(record.col("value"), "#").getItem(0))
                .withColumn("patch", functions.split(record.col("value"), "#").getItem(1))
                .withColumn("time", functions.split(record.col("value"), "#").getItem(2))
                .drop("value");
        StreamingQuery query = result
//                .selectExpr("name")
//                .coalesce(1)
                .writeStream()
//                .outputMode("append")
//                .format("parquet")
//                .option("compression", "snappy")
//                .option("path","hdfs://10.140.0.5:9000/user/huylq78/data_tracking")
//                .option("checkpointLocation","hdfs://10.140.0.5:9000/user/huylq78/data_tracking_checkpoint")
//                .partitionBy("year","month","day","hour")
                .format("console")
                .start();

        query.awaitTermination();

    }
}
