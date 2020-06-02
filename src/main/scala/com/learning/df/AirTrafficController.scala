package com.learning.df

import com.learning.SparkInstance.session
import org.apache.spark.sql.DataFrame

object AirTrafficController {

  def getActualAirTime(flights: DataFrame): DataFrame = {
    import session.implicits._
    flights.select($"air_time" - $"departure_delay" + $"arrival_delay")
      .withColumnRenamed("((air_time - departure_delay) + arrival_delay)", "actual_air_time")
  }

}
