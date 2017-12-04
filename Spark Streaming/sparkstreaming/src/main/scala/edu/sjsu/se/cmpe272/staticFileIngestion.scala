package edu.sjsu.se.cmpe272

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.udf

object staticFileIngestion extends App {

  val inFName = args(0)
  val hiveDB = args(1)
  val hiveTableName = args(2)

  val sc = SparkSession.builder().enableHiveSupport().getOrCreate()

  sc.catalog.listDatabases.show(true)
  sc.catalog.listTables.show(true)


  sc.sql("DROP TABLE IF EXISTS "+hiveDB+"."+hiveTableName)


  val df = sc.read.format("com.databricks.spark.csv").
    option("header","true").
    load("file://"+inFName)

  val toInt    = udf[Int, String]( _.replace("$","").replaceAll(",","").trim().toInt)


  if(inFName.contains("Average-Income"))
    avgIncome()
  else
    defaultTrans()

  def defaultTrans(): Unit ={

    println("**********************************")
    df.write.saveAsTable(hiveDB + "." + hiveTableName)

  }


  def avgIncome(): Unit ={

    val df2 = df.withColumn("PerCapitaIncome",toInt(df("Per-Capita-Income"))).
      withColumn("MedianHouseholdIncome",toInt(df("Per-Capita-Income"))).
      withColumn("MedianFamilyIncome",toInt(df("Per-Capita-Income"))).
      drop("Per-Capita-Income","Median-Household-Income","Median-Family-Income")

    df2.write.saveAsTable(hiveDB + "." + hiveTableName)

  }

}
