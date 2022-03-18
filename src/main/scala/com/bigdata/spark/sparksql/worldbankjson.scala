package com.bigdata.spark.sparksql

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object worldbankjson {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("worldbankjson").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    import spark.implicits._
    import spark.sql
    val data="C:\\Users\\sajadhav\\IdeaProjects\\SparkPoc\\world_bank.json"
    val df=spark.read.format("json").load(data)
    df.show()
   df.printSchema()

  val res=df.withColumn("newcol",lit(1))
     .withColumn("rownum",monotonically_increasing_id())
     .withColumn("supplementprojectflg",when($"supplementprojectflg"==="N","NO").otherwise("YES"))
      .withColumn("theme1Name",$"theme1.Name")
      .withColumn("theme1Percent",$"theme1.percent").drop("theme1")
      .withColumn("theme_namecode", explode($"theme_namecode"))
      .withColumn("theme_namecode_code",$"theme_namecode.code")
      .withColumn("theme_namecode_name",$"theme_namecode.name").drop("theme_namecode")
     .withColumn("sector", explode($"sector"))
     // .withColumn("element",$"sector.element")
      .withColumn("name",$"sector.name")
      .withColumn("Name",$"sector1.Name")
      .withColumn("Percent",$"sector1.Percent")
    //  .withColumn("sector_namecode",explode($"sector"))
      .withColumn("majorsector_percent",explode($"majorsector_percent"))
      .withColumn("name",$"majorsector_percent.name")
      .withColumn("percent",$"majorsector_percent")
      .withColumn("mjsector_namecode",explode($"mjsector_namecode"))
      .withColumn("mjtheme",explode($"mjtheme"))
      .drop("majorsector_percent","mjtheme_namecode","sector")
      .withColumn("idoid",$"_id.$$oid").drop("_id")
    res.printSchema()
    res.show()
    res.createOrReplaceTempView("tab")
    val result=spark.sql("select * from worldbankjson")
    result.show()

//storing data in mysql below code
    /*val host="jdbc:mysql://mysqldb........../mydb"  ---aws url
      res.write.format("jdbc").option("url",host).option("user","mysqluser").option("password","mypass")
      .option("driver","com.mysql.cj.jdbc.Driver").option("dbtable","sparkjson")
        .save()  */

    spark.stop()
  }
}