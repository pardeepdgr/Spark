package com.learning.rdd

import com.learning.spark.SparkInstance.{session, sparkContext}
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

object RddExaminer extends App {

  val rddFromCollection = createRddFromCollection
  rddFromCollection.count()
  rddFromCollection.first()
  rddFromCollection.take(2)
  rddFromCollection.distinct()

  val rddFromCsv = createRddFromCsv
  rddFromCsv.collect().foreach(println)

  val empDF = createDataFrameFromAnRdd
  empDF.printSchema()
  empDF.show()

  private def createDataFrameFromAnRdd = {
    val schema = new StructType()
      .add(StructField("id", StringType, true))
      .add(StructField("name", StringType, true))
      .add(StructField("address", StringType, true))
      .add(StructField("contact", StringType, true))
      .add(StructField("empid", IntegerType, true))
      .add(StructField("deptid", IntegerType, true))

    session.createDataFrame(rddFromCollection, schema)
  }

  private def createRddFromCsv = {
    val path = "src/main/resources/airlines/airlines.csv"
    sparkContext.textFile(path)
  }

  private def createRddFromCollection = {
    val data = Array(Row("1", "Name", "Address", "000-000-0000", 5, 1))
    sparkContext.parallelize(data)
  }
}
