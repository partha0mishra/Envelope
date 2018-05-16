package com.mishra.partha.envelope

/**
 * @author ${user.name}
 */
object SimpleApp {
  def main(args: Array[String]) {
    val logFile = "D:/logs/controller.log"
    val spark = SparkSession.builder.appName("Simple Application").getOrCreate()
    val logData = spark.read.textFile(logFile).cache()
    val numAs = logData.filter(line => line.contains("a")).count()
    val numBs = logData.filter(line => line.contains("b")).count()
    println(s"Lines with a: $numAs, Lines with b: $numBs")
    spark.stop()
  }
}