package com.learning.pay_later

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, desc, row_number}
import org.apache.spark.sql.{DataFrame, Dataset, Row}

object UserSummaryCreator {

  def create(user: DataFrame, transaction: DataFrame, repayment: DataFrame): DataFrame = {

    val userTransaction = user
      .join(transaction
        .withColumnRenamed("created_at", "transaction_time")
        .withColumnRenamed("amount", "transaction_amount"),
        user("user_id") === transaction("user_id"), "left")
      .drop(transaction("user_id"))

    val latestTransaction: DataFrame = getLatestFromGroup(userTransaction, "transaction_time")
      .drop("transaction_amount")
    val totalTransactionAmount: DataFrame = userTransaction.groupBy("user_id").sum("transaction_amount")

    val userRepayment = user
      .join(repayment
        .withColumnRenamed("created_at", "repayment_time")
        .withColumnRenamed("amount", "repayment_amount"),
        user("user_id") === repayment("user_id"), "left")
      .drop(repayment("user_id"))
      .drop("credit_limit")

    val latestRepayment = getLatestFromGroup(userRepayment, "repayment_time")
      .drop("repayment_amount")
    val totalRepaymentAmount = userRepayment.groupBy("user_id").sum("repayment_amount")
    val numberOfRepayment = userRepayment.groupBy("user_id").count().as("number_of_repayments")

    collateUserSummary(
      latestTransaction,
      totalTransactionAmount,
      latestRepayment,
      totalRepaymentAmount,
      numberOfRepayment)
  }

  private def collateUserSummary(latestTransaction: DataFrame,
                                 totalTransactionAmount: DataFrame,
                                 latestRepayment: DataFrame,
                                 totalRepaymentAmount: DataFrame,
                                 numberOfRepayment: Dataset[Row]): DataFrame = {
    latestRepayment.join(totalRepaymentAmount, "user_id").join(numberOfRepayment, "user_id")
      .join(latestTransaction, "user_id")
      .join(totalTransactionAmount, "user_id")
      .withColumnRenamed("count", "number_of_repayments")
      .withColumnRenamed("sum(transaction_amount)", "total_txn_amount")
      .withColumnRenamed("repayment_time", "last_repayment_date")
      .withColumnRenamed("sum(repayment_amount)", "total_repaid_amount")
      .withColumnRenamed("merchant_id", "last_txn_merchant")
      .withColumnRenamed("transaction_time", "last_txn_date ")
      .withColumn("available_credit_limit", col("credit_limit") - col("total_txn_amount") + col("total_repaid_amount"))
      .withColumn("due_amount", col("total_txn_amount") - col("total_repaid_amount"))
  }

  private def getLatestFromGroup(df: DataFrame, colName: String): DataFrame = {
    val windowSpec = Window
      .partitionBy("user_id")
      .orderBy(desc(colName))

    df
      .withColumn("row_number", row_number.over(windowSpec))
      .filter(col("row_number") <= 1)
      .drop("row_number")
  }

}
