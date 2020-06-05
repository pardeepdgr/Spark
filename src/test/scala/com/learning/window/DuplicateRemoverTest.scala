package com.learning.window

import base.TestBootstrap
import base.TestSetup.{init, kill}
import com.learning.SparkInstance.session.implicits._
import com.learning.helper.DataFrameComparator.compareContent

class DuplicateRemoverTest extends TestBootstrap {

  private val dataset = Seq(("Rajesh", 21, "London"), ("Suresh", 28, "California"),
    ("Sam", 26, "Delhi"), ("Rajesh", 21, "Gurgaon"), ("Manish", 29, "Bengaluru"))
    .toDF("Name", "Age", "Location")

  private val expectedDataset = Seq(("Rajesh", 21, "London"), ("Suresh", 28, "California"),
    ("Sam", 26, "Delhi"), ("Manish", 29, "Bengaluru")).toDF("Name", "Age", "Location")

  before {
    init("DuplicateRemoverTest", "local")
  }

  it should "remove duplicate Name, Age" in {
    val actualDataset = DuplicateRemover.columnBasedDistinct(dataset)

    val isSameContent = compareContent(expectedDataset, actualDataset)
    assert(isSameContent)
  }

  after {
    kill
  }

}
