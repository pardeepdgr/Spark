package base

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object TestSetup {

  var session: SparkSession = _

  def init(app: String, master: String) = {
    val conf = new SparkConf()
      .setAppName(app)
      .setMaster(master)

    session = SparkSession
      .builder()
      .config(conf)
      .getOrCreate()

    session.sparkContext.setLogLevel("ERROR")
  }

  def kill() = {
    session.stop()
  }

}
