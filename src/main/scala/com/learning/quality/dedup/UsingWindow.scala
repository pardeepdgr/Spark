package com.learning.quality.dedup

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, desc, row_number}

object UsingWindow {
  private val TEMP_COL = "row"

  def dedup(data: DataFrame): DataFrame = {
    val windowSpec = Window
      .partitionBy("Name", "Age")
      .orderBy(desc("Name"))

    data
      .withColumn(TEMP_COL, row_number.over(windowSpec))
      .filter(col(TEMP_COL) <= 1)
      .drop(TEMP_COL)
  }

}
