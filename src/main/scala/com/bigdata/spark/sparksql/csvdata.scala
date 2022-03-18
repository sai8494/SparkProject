package com.bigdata.spark.sparksql

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object csvdata {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("csvdata").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    import spark.implicits._
    import spark.sql

    val data="C:\\Users\\sajadhav\\IdeaProjects\\SparkPoc\\us-500 .csv"
    val df=spark.read.format("csv").option("header","true").option("inferSchema","true").load(data)
    df.show(5)
    df.printSchema()
    df.createOrReplaceTempView("tab")
//val res=df.filter(col("state")=="CA")
    val res=spark.sql("select * from tab where state=='NY'")
    res.show()
    spark.stop()
  }
}