package com.learning.ds

import base.TestBootstrap
import base.TestSetup.{init, kill, session}
import com.learning.creator.DataFrameCreator.fromCsv
import com.learning.ds.TypedProducts.datasetFromDF
import org.apache.spark.sql.DataFrame

class TypedProductsTest extends TestBootstrap {

  private val PRODUCTS = "src/test/resources/products/product.csv"
  private var products: DataFrame = _

  before {
    init("TypedProductsTest", "local")
    products = fromCsv(session, PRODUCTS)
  }

  it should "create a dataset from dataframe" in {
    datasetFromDF(products).show()
  }

  after {
    kill
  }

}
