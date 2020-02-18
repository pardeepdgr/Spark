package com.learning.rdd

import com.learning.helper.{DataFrameCreator, RDDCreator}
import com.learning.spark.SparkInstance.{session, sparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

object EmployeeAnalyzer extends App {
  val AIRLINE_DATASET_PATH = "src/main/resources/airlines/airlines.csv"

  val schema = new StructType()
    .add(StructField("id", StringType, true))
    .add(StructField("name", StringType, true))
    .add(StructField("address", StringType, true))
    .add(StructField("contact", StringType, true))
    .add(StructField("empid", IntegerType, true))
    .add(StructField("deptid", IntegerType, true))
  val data = Array(Row("1", "Name", "Address", "000-000-0000", 5, 1))

  val rddFromRows: RDD[Row] = RDDCreator.fromRows(sparkContext, data)
  val rddFromCsv: RDD[String] = RDDCreator.fromCsv(sparkContext, AIRLINE_DATASET_PATH)

  rddFromRows.count()
  rddFromRows.first()
  rddFromRows.take(2)
  rddFromRows.distinct()
  rddFromRows.collect().foreach(println)

  DataFrameCreator.fromRdd(session, rddFromRows, schema)
}
