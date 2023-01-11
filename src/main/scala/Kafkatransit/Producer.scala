package scala

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.spark


import java.io.File
import java.util.Properties
import scala.io.Source

object Producer extends App {

// Début___Récupération des fichiers du systemLocal et enregistrement sous forme de String value
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

   //FIN___Récupération des fichiers du systemLocal et enregistrement sous forme de String value

   //DEBUT___Saving Data in Kafka' Topics

   val producerProperties = new Properties()
   producerProperties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"127.0.0.1:9092")
   producerProperties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
   producerProperties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)

   val kafkaProducer = new KafkaProducer[String,String](producerProperties)
   val producerRecordPays = new ProducerRecord[String,String]("pays",DataPays )
   val producerRecordCarto = new ProducerRecord[String,String]("carto",DataCarto )
   val producerRecordTours = new ProducerRecord[String,String]("tours",DataTours )
   val producerRecordSociete = new ProducerRecord[String,String]("societe",DataSociete)

   kafkaProducer.send(producerRecordPays)
   kafkaProducer.send(producerRecordCarto)
   kafkaProducer.send(producerRecordTours)
   kafkaProducer.send(producerRecordSociete)
    //print(producerRecord)
   kafkaProducer.flush()
   kafkaProducer.close()
   //FIN___Saving Data in Kafka' Topics





}