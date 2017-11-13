package edu.sjsu.se.cmpe272

import org.apache.hadoop.yarn.util.RackResolver
import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.log4j.{Level, Logger}

object Streaming extends App {

  System.setProperty("java.security.auth.login.config", "kafka_config.conf")

  Logger.getLogger(classOf[RackResolver]).getLevel
  Logger.getLogger("org").setLevel(Level.ERROR)
  Logger.getLogger("akka").setLevel(Level.OFF)

  val conf = new SparkConf().
     set("spark.driver.host","localhost").
    set("spark.ui.port","4041").setMaster("local[2]").setAppName("DirectKafkaStreaming")
  val ssc = new StreamingContext(conf, Seconds(30))
  val sc = ssc.sparkContext


  sc.setLogLevel("INFO")
  val topic = "test,test2"

  val bs_server = " kafka04-prod02.messagehub.services.us-south.bluemix.net:9093," +
    "kafka03-prod02.messagehub.services.us-south.bluemix.net:9093," +
    "kafka02-prod02.messagehub.services.us-south.bluemix.net:9093," +
    "kafka01-prod02.messagehub.services.us-south.bluemix.net:9093," +
    "kafka05-prod02.messagehub.services.us-south.bluemix.net:9093 "

  //val bs_server = "kafka04-prod02.messagehub.services.us-south.bluemix.net:9093," +
  //  ""
  //Specify number of Receivers you need.
  val numberOfReceivers = 1



  val kafkaProperties: Map[String, String] =
    Map(
      "kafka.topic" -> topic,
      "metadata.broker.list"->bs_server,
      "bootstrap.servers" -> bs_server,
      "kafka.consumer.id" -> "kafka-consumer",
      "security.protocol"-> "SASL_SSL",
       "sasl.mechanism" -> "PLAIN",
      // "ssl.protocol" -> "TLSv1.2",
      // "ssl.enabled.protocols" -> "TLSv1.2",
      "StreamsConfig.REPLICATION_FACTOR_CONFIG" -> "3",
      "connections.max.idle.ms" -> "600000",
      "kafka.user.name" -> "xxxxxx",
      "kafka.user.password" -> "xxxxxxx",
      "api_key" -> "xxxxxxxx",
      "kafka_rest_url" ->  "https://kafka-rest-prod02.messagehub.services.us-south.bluemix.net:443",
      "group.id" -> "kafka-python-console-sample-group",
      "client.id" -> "kafka-python-console-sample-consumer2",
      "key.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
      "value.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
      "enable.auto.commit" -> "true",
      "auto.commit.interval.ms" -> "1000",
      "auto.offset.reset" -> "earliest",
      "sasl.jaas.config" -> "org.apache.kafka.common.security.plain.PlainLoginModule required username='aBK1K5xyorerlBe0' password='HSNyrLogsaif0pd803ayUJLBqZV9EuNp'",
      "api.version.request" -> "True"
      //"security.protocol" -> "SASL_PLAINTEXT",
      //"sasl.mechanism" -> "PLAIN",
      //"ssl.protocol" ->  "TLSv1.2",
      //"ssl.enabled.protocols" -> "TLSv1.2"
    )



  val messages = KafkaUtils.createDirectStream[String, String](
    ssc,
    LocationStrategies.PreferConsistent,
    ConsumerStrategies.Subscribe[String, String](topic.split(",").toSet,kafkaProperties))

  print("Write to HDFS")
  // messages.saveAsTextFiles("hdfs:///tmp/streamout/file","txt")

  val kv = messages.map(a => (a.key(),a.value()))
  println(kv.count())

  val kv_table1 = kv.filter( _._1 == "crime_data" )

  println(kv_table1.count())

  kv_table1.foreachRDD(a => {
    println("Im here")
    println(a.count())
    if(a.count() > 0){
      val ts = System.nanoTime()
      a.map(_._2).saveAsTextFile("hdfs:///tmp/stream-out/crime_data/ts=" + ts)
    }

  })

  ssc.start()
  ssc.awaitTermination()

}
