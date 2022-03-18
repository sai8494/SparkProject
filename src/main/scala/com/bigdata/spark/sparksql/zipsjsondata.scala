package com.bigdata.spark.sparksql

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object zipsjsondata {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("zipsjsondata").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    import spark.implicits._
    import spark.sql

    val data="C:\\Users\\sajadhav\\IdeaProjects\\SparkPoc\\zips.json"
    val df=spark.read.format("json").load(data)
   // df.show()
   // df.printSchema()

    //sql friendly
  /*  df.createOrReplaceTempView("tab")
    val res=spark.sql("select _id id,city,loc[0] lang,loc[1] lati,pop,state from tab")
    res.show()
    res.printSchema()*/

    //scala friendly
    val res=df.withColumnRenamed("_id","id")
      .withColumn("lang",$"loc"(0))
      .withColumn("lati",$"loc"(1))
      .drop($"loc")
    res.show(5)
    res.printSchema()

    spark.stop()
  }
}