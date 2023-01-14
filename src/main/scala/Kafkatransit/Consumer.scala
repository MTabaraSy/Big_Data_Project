package scala

import org.apache.hadoop.shaded.org.eclipse.jetty.websocket.common.frames.DataFrame
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecords, KafkaConsumer}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.time.Duration
import java.util.Properties
import scala.collection.JavaConverters.seqAsJavaListConverter

object Consumer extends App {
  val topic1 = "pays"
  val topic2 = "carto"

  val consumerProperties = new Properties()
  consumerProperties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092")
  consumerProperties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "test-consumer")
  consumerProperties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
  consumerProperties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)
  consumerProperties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)
  consumerProperties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")

  val kafkaConsumerpays = new KafkaConsumer[String,String](consumerProperties)
  val kafkaConsumercarto = new KafkaConsumer[String,String](consumerProperties)
  kafkaConsumerpays.subscribe(List(topic1).asJava)
  kafkaConsumercarto.subscribe(List(topic2).asJava)

  println("key |Message |Partition |Offset")
  /*while (true){
    val polledRecords : ConsumerRecords[String,String] = kafkaConsumerpays.poll(Duration.ofMillis(100))
    if(!polledRecords.isEmpty){
      println(s"polled ${polledRecords.count()} records")
      val recordIterator = polledRecords.iterator()
      while (recordIterator.hasNext){
        val record = recordIterator.next()
        println(s"| ${record.key()} | ${record.value()} | ${record.partition()} | ${record.offset()} |")
      }
    }
  }*/

  val spark = SparkSession
    .builder()
    .master("local")
    .appName("kafka-to-hdfs")
    .config("spark.sql.warehouse.dir", "/user/hive/warehouse")
    .enableHiveSupport()
    .getOrCreate()

  import spark.implicits._

    //Import dans HDFS avec "Structured Streaming"
    val PaysDF = spark
      .read
      .format("kafka")
      .option("kafka.bootstrap.servers", "127.0.0.1:9092")
      .option("subscribe", "pays")
      .load()

    PaysDF.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
      .as[(String, String)]
    PaysDF.write.parquet("hdfs://localhost:9000/DataPays.parquet")

    val CartoDF = spark
      .read
      .format("kafka")
      .option("kafka.bootstrap.servers", "127.0.0.1:9092")
      .option("subscribe", "cartographie")
      .load()
    CartoDF.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
      .as[(String, String)]
    CartoDF.write.parquet(("hdfs://localhost:9000/DataCarto.parquet"))

    val ToursDF = spark
      .read
      .format("kafka")
      .option("kafka.bootstrap.servers", "127.0.0.1:9092")
      .option("subscribe", "tours")
      .load()
    ToursDF.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
    .as[(String, String)]
    ToursDF.write.parquet(("hdfs://localhost:9000/DataTours.parquet"))

    val SocieteDF = spark
      .read
      .format("kafka")
      .option("kafka.bootstrap.servers", "127.0.0.1:9092")
      .option("subscribe", "societe")
      .load()
    SocieteDF.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
    .as[(String, String)]
    SocieteDF.write.parquet(("hdfs://localhost:9000/DataSociete.parquet"))


  val createTabletours = "CREATE EXTERNAL TABLE lightinvestdb.t_tours (company_permalink STRING,funding_round_permalink STRING,funding_round_type STRING,funding_round_code STRING,funded_at STRING,raised_amount_usd STRING) STORED AS PARQUET LOCATION 'hdfs://localhost:9000/DataTours.parquet'"
  spark.sql(createTabletours)

  val createTablesociete = "CREATE EXTERNAL TABLE lightinvestdb.t_societe (permalink STRING,name STRING,homepage_url STRING,category_list STRING,status STRING,country_code STRING,state_code STRING,region STRING,cityfounded_at STRING) STORED AS PARQUET LOCATION 'hdfs://localhost:9000/DataTours.parquet'"
  spark.sql(createTablesociete)



  //FIN___



}