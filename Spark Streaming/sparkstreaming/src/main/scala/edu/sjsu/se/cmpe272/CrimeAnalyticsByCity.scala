package edu.sjsu.se.cmpe272

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.lit

object CrimeAnalyticsByCity extends App{

  val sc = SparkSession.builder().enableHiveSupport().getOrCreate()

  sc.catalog.listDatabases.show(true)
  sc.catalog.listTables.show(true)

  sc.sql("DROP TABLE IF EXISTS crime_etl.chicago_crime_type")
  sc.sql("DROP TABLE IF EXISTS crime_etl.LA_crime_type")
  sc.sql("DROP TABLE IF EXISTS crime_etl.SFO_crime_type")
  sc.sql("DROP TABLE IF EXISTS crime_etl.flattened_crime_data_city")
  sc.sql("msck repair table crime_etl.crime_etl_temp_SFO")
  sc.sql("msck repair table crime_etl.crime_etl_temp_la")
  sc.sql("msck repair table crime_etl.crime_etl_temp_chicago")

  val df = sc.sql("select Description,year,COUNT(*) as crime_count from crime_etl.crime_etl_temp_chicago GROUP BY DESCRIPTION,year")

  df.withColumn("City",lit("Chicago")).write.mode("overwrite").saveAsTable("crime_etl.chicago_crime_type")

  val df2 = sc.sql("select substring(Date_Occurred,LENGTH(Date_Occurred)-3,LENGTH(Date_Occurred)) as year,Crime_Code_Description,COUNT(*) as crime_count from crime_etl.crime_etl_temp_la GROUP BY substring(Date_Occurred,LENGTH(Date_Occurred)-3,LENGTH(Date_Occurred)),Crime_Code_Description")

  df2.withColumn("City",lit("Los Angeles")).write.mode("overwrite").saveAsTable("crime_etl.LA_crime_type")

  val df3 = sc.sql("select substring(Date_Crime,LENGTH(Date_Crime)-3,LENGTH(Date_Crime)) as year,Category,COUNT(*) as crime_count from crime_etl.crime_etl_temp_SFO GROUP BY substring(Date_Crime,LENGTH(Date_Crime)-3,LENGTH(Date_Crime)),Category")

  df3.withColumn("City",lit("San Francisco")).write.mode("overwrite").saveAsTable("crime_etl.SFO_crime_type")

  val df4 = sc.sql("select City, sum(crime_count) as crime_count from crime_etl.chicago_crime_type where year = 2016 group by city")
  val df5 = sc.sql("select City, sum(crime_count) as crime_count from crime_etl.LA_crime_type where year = 2016 group by city")
  val df6 = sc.sql("select City, sum(crime_count) as crime_count from crime_etl.SFO_crime_type where year = 2016 group by city")

  val df7 = df4.union(df5).union(df6)

  val df8 = sc.sql("select City, Median2 as Avg_Rent from crime_etl.rental_prices")

  val df9 = sc.sql("select city, homeless from crime_etl.homelessbycity")

  val df_final = df7.join(df8,"City").join(df9,"city")

  df_final.write.saveAsTable("crime_etl.flattened_crime_data_city")

}
