package com.learning.quality.dedup

import base.TestBootstrap
import base.TestSetup.{init, kill}
import com.learning.SparkInstance.session.implicits._
import com.learning.helper.DataFrameComparator.compareContent

class UsingGroupByTest extends TestBootstrap {

  private val data = Seq(("Vamika", "London", 4), ("Dev", "California", 3),
    ("Dhruv", "Delhi", 2), ("Vamika", "London", 2), ("M", "Berlin", 3))
    .toDF("Name", "Location", "task")

  private val expectedData = Seq(("Vamika", "London", Seq(4, 2)), ("Dev", "California", Seq(3)),
    ("Dhruv", "Delhi", Seq(2)), ("M", "Berlin", Seq(3))).toDF("Name", "Location", "task")

  before {
    init("DedupUsingGroupByTest", "local")
  }

  it should "remove duplicate Name, Age using Window" in {
    val actualDataset = UsingGroupBy.dedup(data)

    val isSameContent = compareContent(expectedData, actualDataset)
    assert(isSameContent)
  }

  after {
    kill()
  }

}
