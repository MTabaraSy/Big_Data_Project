package scala

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer

import java.util.Properties

object Producer extends App {


   val producerProperties = new Properties()
   producerProperties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"127.0.0.1:9092")
   producerProperties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
   producerProperties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)

   val kafkaProducer = new KafkaProducer[String,String](producerProperties)
   val producerRecord = new ProducerRecord[String,String]("Topic1","Mogui Moche deihh")
   kafkaProducer.send(producerRecord)

   kafkaProducer.flush()


}