package scala

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecords, KafkaConsumer}
import org.apache.kafka.common.serialization.StringDeserializer

import java.time.Duration
import java.util.Properties
import scala.collection.JavaConverters.seqAsJavaListConverter

object Consumer extends App {
  val topic = "carto"

  val consumerProperties = new Properties()
  consumerProperties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092")
  consumerProperties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "test-consumer")
  consumerProperties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
  consumerProperties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)
  consumerProperties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)
  consumerProperties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")

  val kafkaConsumer = new KafkaConsumer[String,String](consumerProperties)
  kafkaConsumer.subscribe(List(topic).asJava)

  println("key |Message |Partition |Offset")
  while (true){
    val polledRecords : ConsumerRecords[String,String] = kafkaConsumer.poll(Duration.ofMillis(100))
    if(!polledRecords.isEmpty){
      println(s"polled ${polledRecords.count()} records")
      val recordIterator = polledRecords.iterator()
      while (recordIterator.hasNext){
        val record = recordIterator.next()
        println(s"| ${record.key()} | ${record.value()} | ${record.partition()} | ${record.offset()} |")
      }
    }
  }


}