package scala

import javafx.scene.control.Tab
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.spark
import org.apache.spark.sql.SparkSession

import org.apache.spark.sql.catalyst.dsl.expressions.{DslExpression, StringToAttributeConversionHelper}

import java.io.File
import java.util.Properties
import scala.io.Source

object Producer extends App {

   //InTopic pour le fichier Pays
   val pays = "C:\\Users\\TeiTei\\Documents\\M2BI-2022\\Big_Data\\Projet BG\\datasets\\pays.csv"
   val filePays = new File(pays)
   val linesPays: Iterator[String] = Source.fromFile(filePays).getLines()
   //linesPays.foreach(println)
   val DataPays = linesPays.mkString(",")

   //InTopic pour le fichier Tours
   val tours = "C:\\Users\\TeiTei\\Documents\\M2BI-2022\\Big_Data\\Projet BG\\datasets\\tours.csv"
   val fileTours = new File(tours)
   val linesTours: Iterator[String] = Source.fromFile(fileTours).getLines()
   //linesTours.foreach(println)
   val DataTours = linesTours.mkString

   //Intopic pour le fichier Cartographie
   val carto = "C:\\Users\\TeiTei\\Documents\\M2BI-2022\\Big_Data\\Projet BG\\datasets\\cartographie.csv"
   val fileCarto = new File(carto)
   val linesCarto: Iterator[String] = Source.fromFile(fileCarto).getLines()
   //linesCarto.foreach(println)
   val DataCarto = linesCarto.mkString(";")

   //InTopic pour le fichier Societe
   val societe = "C:\\Users\\TeiTei\\Documents\\M2BI-2022\\Big_Data\\Projet BG\\datasets\\societe.txt"
   val fileSociete = new File(societe)
   val linesSociete: Iterator[String] = Source.fromFile(fileSociete).getLines()
   //linesSociete.foreach(println)
   val DataSociete = linesSociete.mkString



   val producerProperties = new Properties()
   producerProperties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"127.0.0.1:9092")
   producerProperties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
   producerProperties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)

   val kafkaProducer = new KafkaProducer[String,String](producerProperties)
   val producerRecordPays = new ProducerRecord[String,String]("pays",DataPays )
   val producerRecordCarto = new ProducerRecord[String,String]("carto",DataCarto )
   val producerRecordTours = new ProducerRecord[String,String]("testScala",DataTours )

   kafkaProducer.send(producerRecordPays)
   kafkaProducer.send(producerRecordCarto)
   kafkaProducer.send(producerRecordTours)
    //print(producerRecord)
   kafkaProducer.flush()
   kafkaProducer.close()

   val spark = SparkSession
     .builder()
     .master("local")
     .appName("load_data")
     .getOrCreate()

   //val DFpays = spark.read.option("hearder", true).option("inferSchema", true).format("csv").load("C:\\Users\\TeiTei\\Documents\\M2BI-2022\\Big_Data\\Projet BG\\datasets\\pays.csv")
import spark.implicits._
   val cartoDF = spark
     .read
     .format("kafka")
     .option("kafka.bootstrap.servers", "127.0.0.1:9092")
     .option("subscribe", "pays")
     .option("includeHeaders","true")
     .load()
   cartoDF.selectExpr("CAST(key AS STRING)","CAST(value AS STRING)", "headers")
     .as[(String, String, Array[(String, Array[Byte])])]
   cartoDF.write.parquet("hdfs://localhost:9000/DataCarto.parquet")


}