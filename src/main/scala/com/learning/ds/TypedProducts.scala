package com.learning.ds

import org.apache.spark.sql.{DataFrame, Dataset, Encoder, Encoders}

object TypedProducts {

  def datasetFromDF(df: DataFrame): Dataset[Products] = {
    val encoder: Encoder[Products] = Encoders.product[Products]
    df.as(encoder)
      .na.drop("all", Seq("Product", "Category")) // delete records only when both product, category is null
      .na.fill("Unknown", Seq("Product"))
      .na.fill(10000, Seq("Price"))
      .as(encoder) // need to do it again as na api returns DataFrame
  }

}

case class Products(product: String, category: String, price: Double)