package com.learning.word_counter

import com.learning.spark.SparkInstance.session

object WordCounter extends App {
  val SPACE = " "
  val lines = session.sparkContext.parallelize(
    Seq("By the pricking of my thumbs, Something wicked this way comes. Open, locks, Whoever knocks!",
      "Fair is foul, and foul is fair: Hover through the fog and filthy air.",
      "Brevity is the soul of wit."))

  val counts = lines
    .flatMap(line => line.split(SPACE))
    .map(word => (word, 1))
    .reduceByKey(_ + _)

  counts.foreach(println)
}
