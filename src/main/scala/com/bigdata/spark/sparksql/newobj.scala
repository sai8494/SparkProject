package com.bigdata.spark.sparksql

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object newobj {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("newobj").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    import spark.implicits._
    import spark.sql
   /*val data="C:\\Users\\sajadhav\\IdeaProjects\\SparkPoc\\bank-full.csv"
   val output="C:\\Users\\sajadhav\\IdeaProjects\\SparkPoc\\output\\bank-full" */
    val data = args(0)
    val output = args(1)

    //val list=List(1,2,3,4,5,6,7,8,9,10)
   // val d=sc.parallelize(list)
    //val d2=d.map(x=>x*x)
    //val coll=d2.collect.foreach(println)
   // print(coll)
  //  val data="C:\\Users\\sajadhav\\IdeaProjects\\SparkPoc\\bank-full.csv"
    val df=spark.read.format("csv").option("header","true").option("sep",";").option("inferschema","true").load(data)
    //df.show(5)
   // df.printSchema()
    df.createOrReplaceTempView("tab")
    val res=spark.sql("select * from tab where age>80 and balance>60000")
    res.show()
    res.coalesce(1).write.mode(SaveMode.Overwrite).format("csv").option("header","true").save(output)

    spark.stop()
  }
}