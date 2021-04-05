package com.learning.helper

import base.TestBootstrap
import base.TestSetup.init
import com.learning.SparkInstance.session.implicits._
import com.learning.SparkInstance.upper
import com.learning.helper.DataFrameHelper.{combineTwoColumns, doesValueExistsInArrayCol}
import com.learning.helper.DataFrameComparator.compareContent

class DataFrameHelperTest extends TestBootstrap {

  before {
    init("DataFrameHelperTest", "local")
  }

  behavior of "DataFrameHelperTest"

  it should "doesValueExistsInArrayCol" in {
    val df = Seq(Seq("abc", "xyz")).toDF("c1")

    df.withColumn("c2", doesValueExistsInArrayCol(df("c1"), "ab")).show() // false
    df.withColumn("c2", doesValueExistsInArrayCol(df("c1"), "abc")).show() // true
  }

  it should "combine 2 column into a new third column" in {
    val df = Seq(("abc", "xyz")).toDF("c1", "c2")

    combineTwoColumns(df, Array("c1", "c2")).show()
  }

  it should "convert text to upper case using UDF" in {
    val df = Seq((0, "hello"), (1, "world")).toDF("id", "text")

    val actualDf = df.withColumn("text", upper('text))
    val expectedDf = Seq((0, "HELLO"), (1, "WORLD")).toDF("id", "text")

    val isSameContent = compareContent(expectedDf, actualDf)
    assert(isSameContent)
  }

}
