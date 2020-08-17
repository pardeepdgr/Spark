package com.learning.pay_later

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, date_format}

object MerchantPerformanceCalculator {

  def overallAndLastWeekPerf(transaction: DataFrame): DataFrame = {

    val overallMerPerf = transaction.groupBy("merchant_id").count()
      .join(transaction.groupBy("merchant_id").sum("amount"), "merchant_id")
      .withColumnRenamed("count", "total_number_of_txns")
      .withColumnRenamed("sum(amount)", "total_amount_of_txns")

    val lastWeekTransaction = transaction
      .withColumn("week_number", date_format(col("created_at"), "w"))
      .filter(col("week_number") === getLastWeekNumber)
      .drop("week_number")
      .drop("user_id")

    val lastWeekMerPerf = lastWeekTransaction.groupBy("merchant_id").count()
      .join(lastWeekTransaction.groupBy("merchant_id").sum("amount"), "merchant_id")
      .withColumnRenamed("count", "last_week_number_of_txns")
      .withColumnRenamed("sum(amount)", "last_week_amount_of_txns")

    collateMerchantPerformanceSummary(overallMerPerf, lastWeekMerPerf)

  }

  private def collateMerchantPerformanceSummary(overallMerPerf: DataFrame,
                                                lastWeekMerPerf: DataFrame): DataFrame = {
    val lastWeekMerPer = lastWeekMerPerf.withColumnRenamed("merchant_id", "id")

    overallMerPerf
      .join(lastWeekMerPer,
        overallMerPerf("merchant_id") === lastWeekMerPer("id"),
        "left")
      .drop("id")
  }

  private def getLastWeekNumber: Int = {
    Integer.parseInt(new SimpleDateFormat("w").format(new Date())) - 1
  }

}
