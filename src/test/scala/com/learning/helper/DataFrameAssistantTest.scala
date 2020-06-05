package com.learning.helper

import base.TestBootstrap
import base.TestSetup.init
import com.learning.SparkInstance.session.implicits._
import com.learning.helper.DataFrameAssistant.doesValueExistsInArrayCol

class DataFrameAssistantTest extends TestBootstrap {

  before {
    init("DataFrameAssistantTest", "local")
  }

  behavior of "DataFrameAssistantTest"

  it should "doesValueExistsInArrayCol" in {
    val df = Seq(Seq("abc", "xyz")).toDF("c1")

    df.withColumn("c2", doesValueExistsInArrayCol(df("c1"), "ab")).show() // false
    df.withColumn("c2", doesValueExistsInArrayCol(df("c1"), "abc")).show() // true
  }

}
