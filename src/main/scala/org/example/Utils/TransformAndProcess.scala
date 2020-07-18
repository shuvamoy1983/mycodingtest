package org.example.Utils

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.example.Utils.Connection.{getSparkSession, init}
import org.example.Utils.helperClass._
import org.apache.spark.sql.functions.udf

object TransformAndProcess {

  val matchpattern=udf ((s: Any) =>matchPattern(s).toString)

  def Barley(dataframe: DataFrame): DataFrame={
    createUniqueId(dataframe).select(
      col("ID"),
      col("year"),
      trim(col("harvest")).as("harvest"))

  }

  def Beef(dataframe: DataFrame): DataFrame={
    createUniqueId(dataframe).select(
      col("ID"),
      col("year"),
      trim(col("Slaughter")).as("Slaughter"))

  }

  def Corn(dataframe: DataFrame): DataFrame={
    createUniqueId(dataframe).select(
      col("ID"),
      col("year"),
      trim(col("harvest")).as("harvest"))

  }

  def Cotton(dataframe: DataFrame): DataFrame={
    createUniqueId(dataframe).select(
      col("ID"),
      col("year"),
      trim(col("harvest")).as("harvest"))

  }
  getSparkSession.udf.register(
    "pgender",
    (s: String) =>
      if (List("f", "female", "woman").contains(s.toLowerCase)) "Female"
      else "Male"
  )

  def mergeOnBarleyAndCompute(dataFrame1: DataFrame,dataFrame2: DataFrame): DataFrame={
    val df=dataFrame1.join(dataFrame2, "ID")
        .select(dataFrame1("ID"),
          dataFrame1("year"),
          dataFrame1("harvest").cast("Double").as("world_barley_harvest"),
          dataFrame2("harvest").cast("Double").as("US_barley_harvest")
        )

      val rslt=df.withColumn("US_barley_contribution",
        (df("US_barley_harvest")/df("world_barley_harvest"))*100)
        .drop("US_barley_harvest")
    rslt
  }

  def mergeOnBeefAndCompute(dataFrame1: DataFrame,dataFrame2: DataFrame): DataFrame={
    val df=dataFrame1.join(dataFrame2, "ID")
      .select(dataFrame1("ID"),
        dataFrame1("year"),
        dataFrame2("Slaughter"),
        dataFrame1("Slaughter").cast("Double").as("world_beef_slaughter"),
        matchpattern(dataFrame2("Slaughter")).cast("Double").as("US_beef_slaughter")
      )

    val rslt=df.withColumn("US_beef_Slaughter_contribution",
      (df("US_beef_slaughter")/df("world_beef_slaughter"))*100)
      .drop("US_beef_slaughter").drop("Slaughter")
    rslt
  }
}
