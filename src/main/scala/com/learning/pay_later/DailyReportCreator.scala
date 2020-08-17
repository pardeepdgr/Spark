package com.learning.pay_later

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, countDistinct, desc, to_date}

object DailyReportCreator {

  def create(transaction: DataFrame): DataFrame = {

    val dateGroupedTx = transaction
      .withColumn("date", to_date(col("created_at"), "yyyy-MM-dd"))
      .drop("created_at")
      .orderBy(desc("date"))
      .groupBy("date")

    dateGroupedTx.sum("amount")
      .join(dateGroupedTx.agg(countDistinct("user_id")), "date")
      .join(dateGroupedTx.agg(countDistinct("merchant_id")), "date")
      .withColumnRenamed("sum(amount)", "transacted_amount")
      .withColumnRenamed("count(DISTINCT user_id)", "number_of_users_transacted")
      .withColumnRenamed("count(DISTINCT merchant_id)", "total_number_of_merchants")
  }

}
