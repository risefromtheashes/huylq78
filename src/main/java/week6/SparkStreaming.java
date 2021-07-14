package week6;



import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.streaming.Trigger;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import week6.datatracking.Datatracking;
import org.apache.spark.sql.functions.*;
import java.io.IOException;
import java.sql.Timestamp;
import java.util.concurrent.TimeoutException;

public class SparkStreaming {
    public static Timestamp getTime(long time){
        Timestamp rand = new Timestamp(time);
        return rand;
    }
    public static void main(String[] args) throws IOException, InterruptedException, TimeoutException, StreamingQueryException {
        //        SparkSession spark = SparkSession
//                .builder()
//                .appName("Spark Kafka Integration using Structured Streaming")
//                .master("local")
//                .getOrCreate();
//        spark.sparkContext().setLogLevel("ERROR");
//        StructType schema = DataTypes.createStructType(new StructField[] {
//                DataTypes.createStructField("version", DataTypes.StringType, false),
//                DataTypes.createStructField("name", DataTypes.StringType, false),
//                DataTypes.createStructField("timestamp", DataTypes.LongType, false),
//                DataTypes.createStructField("phone_id", DataTypes.StringType, false),
//                DataTypes.createStructField("lon", DataTypes.LongType, false),
//                DataTypes.createStructField("lat", DataTypes.LongType, false)
//        });
//
//        Dataset<Row> df = spark
//                .readStream()
//                .format("kafka")
//                .option("kafka.bootstrap.servers" ,"10.140.0.3:9092")
//                .option("startingOffsets", "earliest")
//                .option("subscribe", "testproto")
//                .option("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
//                .option("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer")
//                .load()
////                .selectExpr("CAST(value AS STRING) as message")
//                ;
//        Dataset<byte[]>words = df.select("value").as(Encoders.BINARY());
//        Dataset<String> object = words.map((MapFunction<byte[], String>)
//                s->Datatracking.DataTracking.parseFrom(s).getName(),Encoders.STRING());
//
//        //        df.write().parquet("hdfs://10.140.0.5:9000/user/huylq78/data_tracking");
//        StreamingQuery query = object.writeStream().format("console").start();
//        query.awaitTermination();

        SparkSession spark = SparkSession.builder()
                .appName("SparkKafka")
//                .master("local")
                .getOrCreate();
        spark.sparkContext().setLogLevel("ERROR");
        Dataset<Row> df = spark.readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers","10.140.0.3:9092")
                .option("subscribe","testproto1")
                .option("group.id","group1")
                .option("startingOffsets", "earliest")
                .option("auto.offset.reset","true")
                .option("value.serializer","org.apache.kafka.common.serialization.ByteArrayDeserializer")
                .load();
        Dataset<byte[]>words = df.select("value").as(Encoders.BINARY());
        Dataset<String> object = words.map((MapFunction<byte[], String>)
                s-> (Datatracking.DataTracking.parseFrom(s).getVersion()+
                        "#"+ Datatracking.DataTracking.parseFrom(s).getName()+
                        "#"+ Datatracking.DataTracking.parseFrom(s).getTimestamp()+
                        "#"+ Datatracking.DataTracking.parseFrom(s).getPhoneId()+
                        "#"+ Datatracking.DataTracking.parseFrom(s).getLon()+
                        "#"+ Datatracking.DataTracking.parseFrom(s).getLat()+
                        "#"+ (getTime(Datatracking.DataTracking.parseFrom(s).getTimestamp()).getYear()+1900)+
                        "#"+ (getTime(Datatracking.DataTracking.parseFrom(s).getTimestamp()).getMonth()+1)+
                        "#"+ getTime(Datatracking.DataTracking.parseFrom(s).getTimestamp()).getDate()+
                        "#"+ getTime(Datatracking.DataTracking.parseFrom(s).getTimestamp()).getHours()
                ),Encoders.STRING());

        Dataset<Row> result = object.withColumn("version", functions.split(object.col("value"), "#").getItem(0))
                .withColumn("name", functions.split(object.col("value"), "#").getItem(1))
                .withColumn("timestamp", functions.split(object.col("value"), "#").getItem(2))
                .withColumn("phone_id", functions.split(object.col("value"), "#").getItem(3))
                .withColumn("lon", functions.split(object.col("value"), "#").getItem(4))
                .withColumn("lat", functions.split(object.col("value"), "#").getItem(5))
                .withColumn("year", functions.split(object.col("value"), "#").getItem(6))
                .withColumn("month", functions.split(object.col("value"), "#").getItem(7))
                .withColumn("day", functions.split(object.col("value"), "#").getItem(8))
                .withColumn("hour", functions.split(object.col("value"), "#").getItem(9))
                .drop("value");

// result.join(tmp, expr)
        StreamingQuery query = result
                .selectExpr("CAST(name AS STRING)","CAST(version AS STRING)","CAST(timestamp AS STRING)","CAST(phone_id AS STRING)","CAST(lon AS STRING)","CAST(year AS STRING)","CAST(month AS STRING)"
                        ,"CAST (day AS STRING)","CAST(hour AS STRING)")
                .coalesce(1)
                .writeStream()
                .outputMode("append")
                .format("parquet")
                .option("compression", "snappy")
                .option("path","hdfs://10.140.0.5:9000/user/huylq78/data_tracking")
                .option("checkpointLocation","hdfs://10.140.0.5:9000/user/huylq78/data_tracking_checkpoint")
                .partitionBy("year","month","day","hour")
                .start();

        query.awaitTermination();

    }
}
