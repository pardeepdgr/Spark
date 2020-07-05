package com.learning.word_counter

import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, Partitioner, RangePartitioner}

object WordCounter {
  private val SPACE = " "
  private val NUMBER_OF_PARTITIONS = 3

  def usingDefaultHashPartitioning(text: RDD[String]): RDD[(String, Int)] = {
    text
      .flatMap(line => line.split(SPACE))
      .map(word => (word, 1))
      .partitionBy(new HashPartitioner(NUMBER_OF_PARTITIONS))
      .reduceByKey(_ + _)
  }

  def usingRangePartitioning(text: RDD[String]): RDD[(String, Int)] = {
    val words: RDD[(String, Int)] = text
      .flatMap(line => line.split(SPACE))
      .map(word => (word, 1))

    words
      .partitionBy(new RangePartitioner(NUMBER_OF_PARTITIONS, words))
      .reduceByKey(_ + _)
  }

  def usingCustomPartitioning(text: RDD[String]): RDD[(String, Int)] = {
    text
      .flatMap(line => line.split(SPACE))
      .map(word => (word, 1))
      .partitionBy(new CustomPartitioner(NUMBER_OF_PARTITIONS))
      .reduceByKey(_ + _)
  }

}

class CustomPartitioner(numberOfPartitions: Int) extends Partitioner {
  override def numPartitions: Int = numberOfPartitions

  /** return partition ID {from 0 to numPartitions-1} */
  override def getPartition(key: Any): Int = if (key.toString.equals("custom")) 0 else 1

  /** check whether two of your RDDs are Partitioned in the same way or not */
  override def equals(partition: Any): Boolean = partition match {
    case other: CustomPartitioner => other.numPartitions == numPartitions
    case _ => false
  }
}