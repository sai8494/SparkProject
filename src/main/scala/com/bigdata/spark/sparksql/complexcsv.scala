package com.bigdata.spark.sparksql

import org.apache.spark.sql._

object complexcsv {
  def main(args: Array[String]){
    val spark = SparkSession.builder.master("local[*]").appName("complexcsv").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")


    val data="C:\\Users\\sajadhav\\IdeaProjects\\SparkPoc\\10000Records.csv"
   // val output="C:\\Users\\sajadhav\\IdeaProjects\\SparkPoc\\output\\complexdata"
    val df=spark.read.format("csv").option("header","true").option("inferSchema","true").load(data)
   val reg="[^a-zA-Z]"
    val cols = df.columns.map(x=>x.replaceAll(reg,""))
    val ndf=df.toDF(cols:_*)
    ndf.show(5)
    ndf.printSchema()
    ndf.createOrReplaceTempView("tab")
    val res=spark.sql("select * from tab where EMail like '%gmail.com'")
    res.show(5)


    spark.stop()
  }
}