package org.example.Utils

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession


object Connection {

  var spark: Option[SparkSession] = None

  def init() ={
    spark = Some(createSparkSession("Starting my spark Test"))
  }

  def createSparkSession(appName: String): SparkSession = {
    val conf = new SparkConf()
      .setAppName("Connecting to Spark")

    val session = SparkSession.builder().appName(appName).config(conf).master("local").getOrCreate()
    session
  }

  def getSparkSession: SparkSession = {
    if (spark.isDefined) {
      spark.get
    }
    else {
      throw new Exception(s"Session Configuration Must Be Set")
    }
  }
}
