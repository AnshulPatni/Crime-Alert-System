package edu.sjsu.se.cmpe272

import org.apache.spark.sql.SparkSession

object CrimeAnalyticsByState extends App {

  val sc = SparkSession.builder().enableHiveSupport().getOrCreate()

  sc.catalog.listDatabases.show(true)
  sc.catalog.listTables.show(true)

  sc.sql("DROP TABLE IF EXISTS crime_etl.flattened_crime_data")

  val df = sc.sql("select lower(state) as state,avg(medianhouseholdincome) as avg_median_income from crime_etl.avg_income group by state")

  val df2 = sc.sql("select lower(state) as state,  (100 - high_school_grad_percentage) as college_drop_out_percent from crime_etl.graduationbystate")

  val df3 = sc.sql("select lower(state) as state , total_homeless2 as homeless from crime_etl.homelessbystate")

  val df4 = sc.sql("select lower(state) as state,sum(violent_crime) as violent_crime from" +
    " crime_etl.crimebycity group by state")
  val df_final = df.join(df2,"state").join(df3,"state").join(df4,"state")



  df_final.write.saveAsTable("crime_etl.flattened_crime_data")


}
