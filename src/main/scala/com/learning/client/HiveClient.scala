package com.learning.client

import com.learning.SparkInstance.session
import org.apache.spark.sql.{DataFrame, SaveMode}

object HiveClient {

  def read(db: String, table: String, columns: Seq[String] = Seq("*"), condition: String = "1=1"): DataFrame = {

    session.sql(s"select ${columns.mkString(",")} from $db.$table where $condition")
  }

  def write(df: DataFrame, db: String, table: String): Unit = {

    df.write.mode(SaveMode.Overwrite).insertInto(db + "." + table)
  }

}
