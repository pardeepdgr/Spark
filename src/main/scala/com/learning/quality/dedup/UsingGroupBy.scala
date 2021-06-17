package com.learning.quality.dedup

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.collect_list

object UsingGroupBy {

  def dedup(data: DataFrame): DataFrame =
    data
      .groupBy("Name", "Location")
      .agg(collect_list("task") as "tasks")

}
