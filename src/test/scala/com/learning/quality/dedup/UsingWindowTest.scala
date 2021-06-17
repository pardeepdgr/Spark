package com.learning.quality.dedup

import base.TestBootstrap
import base.TestSetup.{init, kill}
import com.learning.SparkInstance.session.implicits._
import com.learning.helper.DataFrameComparator.compareContent

class UsingWindowTest extends TestBootstrap {

  private val data = Seq(("Rajesh", 21, "London"), ("Suresh", 28, "California"),
    ("Sam", 26, "Delhi"), ("Rajesh", 21, "Gurgaon"), ("Manish", 29, "Bengaluru"))
    .toDF("Name", "Age", "Location")

  private val expectedData = Seq(("Rajesh", 21, "London"), ("Suresh", 28, "California"),
    ("Sam", 26, "Delhi"), ("Manish", 29, "Bengaluru")).toDF("Name", "Age", "Location")

  before {
    init("DedupUsingWindowTest", "local")
  }

  it should "remove duplicate Name, Age using Window" in {
    val actualDataset = UsingWindow.dedup(data)

    val isSameContent = compareContent(expectedData, actualDataset)
    assert(isSameContent)
  }

  after {
    kill()
  }

}
