package com.learning.partitioning

import base.TestBootstrap
import base.TestSetup.{init, kill, session}
import org.apache.spark.rdd.RDD


class WordCounterTest extends TestBootstrap {

  private val data = Seq("By the pricking of my thumbs, Something wicked this way comes. Open, locks, Whoever knocks!",
    "Fair is foul, and foul is fair: Hover through the fog and filthy air.",
    "Brevity is the soul of wit.")

  before {
    init("WordCounterTest", "local")
  }

  behavior of "RDD partitioning"

  it should "get word count in a partitioned rdd using default partitioning" in {
    val text: RDD[String] = session.sparkContext.parallelize(data)

    WordCounter.usingDefaultHashPartitioning(text).foreach(println)
  }

  it should "get word count in a partitioned rdd using range partitioning" in {
    val text: RDD[String] = session.sparkContext.parallelize(data)

    // ranges are determined by sampling the content of the RDD
    WordCounter.usingRangePartitioning(text).foreach(println)
  }

  it should "get word count in a partitioned rdd using custom partitioning" in {
    val text: RDD[String] = session.sparkContext.parallelize(data)

    WordCounter.usingCustomPartitioning(text).foreach(println)
  }

  after {
    kill
  }

}
