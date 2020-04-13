package base

import org.apache.spark.sql.{DataFrame, SparkSession}

object TestHelper {

  def readCSV(session: SparkSession, path: String): DataFrame = {
    session.read
      .format("csv")
      .option("header", "true")
      .load(path)
  }

}
