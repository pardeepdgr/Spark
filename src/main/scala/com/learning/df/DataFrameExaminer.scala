package com.learning.df

import com.learning.spark.SparkInstance.session

object DataFrameExaminer extends App {
  val dfFromRange = session.range(5)
  dfFromRange.show()

}
