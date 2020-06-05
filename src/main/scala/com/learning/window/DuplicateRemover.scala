package com.learning.window

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, desc, row_number}

object DuplicateRemover {
  private val TEMP_COL = "row"

  def columnBasedDistinct(dataset: DataFrame): DataFrame = {
    val windowSpec = Window
      .partitionBy("Name", "Age")
      .orderBy(desc("Name"))

    dataset
      .withColumn(TEMP_COL, row_number.over(windowSpec))
      .filter(col(TEMP_COL) <= 1)
      .drop(TEMP_COL)
  }

}
