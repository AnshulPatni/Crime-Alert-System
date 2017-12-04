package edu.sjsu.se.cmpe272

import java.sql.{Connection, DriverManager}

import org.apache.hadoop.yarn.util.RackResolver
import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.JdbcRDD

object Streaming extends App {

  System.setProperty("java.security.auth.login.config", "kafka_config.conf")

  Logger.getLogger(classOf[RackResolver]).getLevel
  Logger.getLogger("org").setLevel(Level.INFO)
  Logger.getLogger("akka").setLevel(Level.OFF)

  val conf = new SparkConf().
     set("spark.driver.host","localhost").
    set("spark.ui.port","4041").setMaster("local[2]").setAppName("DirectKafkaStreaming")
  val ssc = new StreamingContext(conf, Seconds(30))
  val sc = ssc.sparkContext
  val ts = System.nanoTime()

  sc.setLogLevel("DEBUG")
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
     // "metadata.broker.list"->bs_server,
      "bootstrap.servers" -> bs_server,
      "kafka.consumer.id" -> "kafka-consumer",
      "security.protocol"-> "SASL_SSL",
       "sasl.mechanism" -> "PLAIN",
      // "ssl.protocol" -> "TLSv1.2",
      // "ssl.enabled.protocols" -> "TLSv1.2",
      "StreamsConfig.REPLICATION_FACTOR_CONFIG" -> "3",
      "connections.max.idle.ms" -> "600000",
      "kafka.user.name" -> "xxxxxxx",
      "kafka.user.password" -> "xxxxxx",
      "api_key" -> "xxxxxx",
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

  val kv_table1 = kv.filter( _._1 == "chicago" )
  val kv_table2 = kv.filter( _._1 == "sfo" )
  val kv_table3 = kv.filter( _._1 == "la" )
  val kv_table4 = kv.filter( _._1 == "rds" )



  // connect to the database named "mysql" on the localhost
  val driver = "com.mysql.jdbc.Driver"
  val url = "jdbc:mysql://xxxxxxxx"
  val username = "communityadmin"
  val password = "sjsu1234"
  var connection:Connection = _

  kv_table1.foreachRDD(a => {
    println("Im here 2")
    println(a.count())
    if(a.count() > 0){
      val ts = System.nanoTime()
      a.map(_._2).saveAsTextFile("hdfs:///tmp/stream-out/crime_data/chicago/ts=" + System.nanoTime())

    }

  })

  kv_table2.foreachRDD(a => {
    println("Im here 2")
    println(a.count())
    if(a.count() > 0){
      val ts = System.nanoTime()
      a.map(_._2).saveAsTextFile("hdfs:///tmp/stream-out/crime_data/SFO/ts=" + System.nanoTime())
    }



   /* a.map(_._2).foreach(st => {
       Class.forName(driver)
       val connection2 = DriverManager.getConnection(url, username, password)
       val statement = connection2.createStatement
       val split = st.split("\\|")
       //print(split(0)+split(1)+split(2)+split(3)+split(4))
       statement.executeUpdate("INSERT INTO CRIME.ALERTS(City, IncidntNum , Category , Descript , CrimeDate , CrimeTime , PdDistrict , Address , X_Coordinate , Y_Coordinate , Notified ) values ('SFO','"+split(0)+"','"+split(1)+"','"+split(2)+"','"+split(4)+"','"+split(5)+"','"+split(6)+"','"+split(8)+"','"+split(9)+"','"+split(10)+"','no')")
       connection2.close()
   })*/


  })

  kv_table4.foreachRDD(a => {
    println("Im here 4")

    a.map(_._2).foreach(st => {
      Class.forName(driver)
      val connection2 = DriverManager.getConnection(url, username, password)
      val statement = connection2.createStatement
      val split = st.split("\\|")
      //print(split(0)+split(1)+split(2)+split(3)+split(4))
      statement.executeUpdate("INSERT INTO CRIME.ALERTS(City, IncidntNum , Category , Descript , CrimeDate , CrimeTime , PdDistrict , Address , X_Coordinate , Y_Coordinate , Notified ) values ('"+split(0)+"','"+split(1)+"','"+split(2)+"','"+split(3)+"','"+split(5)+"','"+split(6)+"','"+split(7)+"','"+split(9)+"','"+split(10)+"','"+split(11)+"','no')")
      connection2.close()
    })


  })

  kv_table3.foreachRDD(a => {
    println("Im here 3")
    println(a.count())
    if(a.count() > 0){

      a.map(_._2).saveAsTextFile("hdfs:///tmp/stream-out/crime_data/LA/ts=" + System.nanoTime())
    }


  })



  ssc.start()
  ssc.awaitTermination()

  def createConnection(): Connection ={
    Class.forName(driver)
    DriverManager.getConnection(url, username, password)
  }
}
