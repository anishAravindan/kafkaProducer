import jdk.nashorn.internal.ir.debug.ObjectSizeCalculator.getObjectSize
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}

import java.util.Properties
import java.util.concurrent.TimeoutException
import scala.io.Source


  object kafkaproducer {

    def main(args:Array[String]): Unit ={

      val fileName = "data/data.csv"
      val topicName = "kafkatest2"
      val producer = getKafkaProducer(topicName)

      var count = 0
      var size : Double = 0
      val t = System.currentTimeMillis()

      //  ***Fetch the details from csv file and producing messages to Kafka topic***
      for (line <- Source.fromFile(fileName).getLines()
           if count < 10000) {
        count +=1
        val record = new ProducerRecord[String,String](topicName,2,null,line)
        size += getObjectSize(record)

        try{
          producer.send(record)
        }
        catch {
          case e: TimeoutException => println(s"Error occurred sending : $record")
        }
      }

      producer.flush()
      val duration = (System.currentTimeMillis() - t) / 1000
      producer.close()

      println("\n \nPERFORMANCE PARAMETERS : \n ")
      println(f"Message size in MB : ${size / (1024 * 1024)}%.2f MB")
      println(f"Time taken to send $count messages :$duration%.2f seconds")
      println(f"Data Velocity : ${count/duration}%.2f Messages/second")
      println(f"Data Velocity : ${size/(duration*1024*1024)}%.2f MB/second")

    }

    //  ****Kafka Producer Configurations***

    def getKafkaProducer(topic: String): KafkaProducer[String, String] = {
      val props = new Properties()

      //    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.59.38:6667")
      props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
      props.put(ProducerConfig.CLIENT_ID_CONFIG,"kafkaProducer")
      props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer")
      props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer")
      new KafkaProducer[String, String](props)
    }


  }


