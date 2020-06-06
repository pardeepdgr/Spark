package com.learning.helper

import base.TestBootstrap
import com.learning.SparkInstance.session.implicits._
import com.learning.helper.DataFrameComparator.compareContent

class CustomUdfTest extends TestBootstrap {

  behavior of "CustomUdfTest"

  it should "convert text to upper case using UDF" in {
    val df = Seq((0, "hello"), (1, "world")).toDF("id", "text")

    import org.apache.spark.sql.functions.udf
    val upper = udf(CustomUdf.upper)

    val actualDf = df.withColumn("text", upper('text))
    val expectedDf = Seq((0, "HELLO"), (1, "WORLD")).toDF("id", "text")

    val isSameContent = compareContent(expectedDf, actualDf)
    assert(isSameContent)
  }

}
