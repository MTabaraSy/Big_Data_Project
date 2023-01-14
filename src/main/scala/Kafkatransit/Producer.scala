package scala

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.spark
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.io.File
import java.util.Properties
import scala.io.Source

object Producer extends App {

// Début
   //write data in kafka topics
   //load data in dataframe
   val spark = SparkSession
     .builder()
     .master("local")
     .appName("load_data")
     .getOrCreate()

   val DFpays : DataFrame = spark
     .read
     .option("header", "true")
     .option("inferSchema", "true")
     .format("csv")
     .load("C:\\Users\\TeiTei\\Documents\\M2BI-2022\\Big_Data\\Projet BG\\datasets\\pays.csv")
   //DFpays.show()

   val DFcarto: DataFrame = spark
     .read
     .option("header", "true")
     .option("inferSchema", "true")
     .format("csv")
     .load("C:\\Users\\TeiTei\\Documents\\M2BI-2022\\Big_Data\\Projet BG\\datasets\\cartographie.csv")
   //DFcarto.show()

   val DFtours : DataFrame = spark
     .read
     .option("header","true")
     .option("inferSchema","true")
     .format("csv")
     .load("C:\\Users\\TeiTei\\Documents\\M2BI-2022\\Big_Data\\Projet BG\\datasets\\tours.csv")
   //DFtours.show()

   val DFsociete : DataFrame = spark
     .read
     .option("header", "true")
     .option("inferSchema", "true")
     .format("csv")
     .option("delimiter","\t")
     .load("C:\\Users\\TeiTei\\Documents\\M2BI-2022\\Big_Data\\Projet BG\\datasets\\societe.txt")
   //DFsociete.show()


   val producerProperties = new Properties()
   producerProperties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092")
   producerProperties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
   producerProperties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)

   //write in kafka using rdd
   DFpays.rdd.foreachPartition { partition =>
      val kafkaProducerPays = new KafkaProducer[String, String](producerProperties)
      partition.foreach { row =>
         val linepays = new ProducerRecord[String, String]("pays", row.mkString(","))
         kafkaProducerPays.send(linepays)
      }
      kafkaProducerPays.flush()
      kafkaProducerPays.close()
   }

   DFcarto.rdd.foreachPartition { partition =>
      val kafkaProducerCarto = new KafkaProducer[String, String](producerProperties)
      partition.foreach { row =>
         val lineCarto = new ProducerRecord[String, String]("cartographie", row.mkString(","))
         kafkaProducerCarto.send(lineCarto)
      }
      kafkaProducerCarto.flush()
      kafkaProducerCarto.close()
   }

   DFtours.rdd.foreachPartition { partition =>
      val kafkaProducerTours = new KafkaProducer[String, String](producerProperties)
      partition.foreach { row =>
         val linetours = new ProducerRecord[String, String]("tours", row.mkString(","))
         kafkaProducerTours.send(linetours)
      }
      kafkaProducerTours.flush()
      kafkaProducerTours.close()
   }

   DFsociete.rdd.foreachPartition { partition =>
      val kafkaProducerSociete = new KafkaProducer[String, String](producerProperties)
      partition.foreach { row =>
         val linesociete = new ProducerRecord[String, String]("societe", row.mkString(","))
         kafkaProducerSociete.send(linesociete)
      }
      kafkaProducerSociete.flush()
      kafkaProducerSociete.close()
   }

//FIN
}