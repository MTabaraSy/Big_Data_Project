package scala

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecords, KafkaConsumer}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.sql.SparkSession

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
  while (true){
    val polledRecords : ConsumerRecords[String,String] = kafkaConsumerpays.poll(Duration.ofMillis(100))
    if(!polledRecords.isEmpty){
      println(s"polled ${polledRecords.count()} records")
      val recordIterator = polledRecords.iterator()
      while (recordIterator.hasNext){
        val record = recordIterator.next()
        println(s"| ${record.key()} | ${record.value()} | ${record.partition()} | ${record.offset()} |")
      }
    }
  }


  //Début___Transfer data from kafka to Hdfs and hive
  val spark = SparkSession
    .builder()
    .master("local")
    .appName("load_data")
    .enableHiveSupport()
    .getOrCreate()

  //val DFpays = spark.read.option("hearder", true).option("inferSchema", true).format("csv").load("C:\\Users\\TeiTei\\Documents\\M2BI-2022\\Big_Data\\Projet BG\\datasets\\pays.csv")

  import spark.implicits._

  //Import dans HDFS avec "Structured Streaming"
  val cartoDF = spark
    .read
    .format("kafka")
    .option("kafka.bootstrap.servers", "127.0.0.1:9092")
    .option("subscribe", "pays")
    .option("includeHeaders", "true")
    .load()
  cartoDF.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)", "headers")
    .as[(String, String, Array[(String, Array[Byte])])]
  cartoDF.write.parquet("hdfs://localhost:9000/DataPays.parquet")


  val dbName = "LightInvestDB"
  val tablepays = "param_pays"
  val tablecarto = "t_carto"
  val creat_DB = "CREATE DATABASE if not exists LightInvestDB; "
  val creat_tablepays = "CREATE EXTERNAL TABLE if not exists " + dbName + "." + tablepays + " ( pays STRING ) STORED AS PARQUET LOCATION 'hdfs://localhost:9000/user/hive/warehouse';"
  val creat_tablecarto = "CREATE EXTERNAL TABLE if not exists LightInvestDB.t_carto(liste_categorie STRING, automobile_et_sports STRING, blancs STRING, technologies_propres_ou_semi_conducteurs STRING,divertissement STRING, sante STRING,fabrication STRING,media STRING, recherche_et_messagerie STRING, autres STRING)" +
    "STORED AS PARQUET LOCATION 'hdfs://localhost:9000/user/hive/warehouse';"

  spark.sql(creat_DB)
  spark.sql(creat_tablepays)
  spark.sql(creat_tablecarto)

  //val execc = "LOAD DATA LOCAL INPATH 'hdfs://localhost:9000/DataPays' OVERWRITE INTO TABLE LightInvestDB.param_pays;"
  //spark.sql(execc)


}