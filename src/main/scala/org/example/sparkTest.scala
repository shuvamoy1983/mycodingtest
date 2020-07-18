package org.example

import com.typesafe.config.ConfigFactory
import org.example.Utils.helperClass._
import org.example.Utils.TransformAndProcess._
import org.apache.spark.sql.functions._
import org.example.Utils.Connection.init

object sparkTest {
  def main(args: Array[String]): Unit = {

    init()
    val config = ConfigFactory.load("application.conf").getConfig("resourceFile")
    val CornInfo = config.getConfig("Corn")
    val CottonInfo = config.getConfig("Cotton")
    val BarleyInfo = config.getConfig("Barley")
    val BeefInfo = config.getConfig("Beef")

    val WorldBarleyData=Barley(readCsvFileInSpark(BarleyInfo.getString("WorldBarleyInfo")))
    val UsBarleyData=Barley(readCsvFileInSpark(BarleyInfo.getString("UsBarleyInfo")))
    val BarleyData=mergeOnBarleyAndCompute(WorldBarleyData,UsBarleyData)


    val WorldBeefData=Beef(readCsvFileInSpark(BeefInfo.getString("WorldBeefInfo")))
    val UsBeefData=Beef(readCsvFileInSpark(BeefInfo.getString("UsBeefInfo")))
    val BeefData=mergeOnBeefAndCompute(WorldBeefData,UsBeefData)


    val WorldCornData=Corn(readCsvFileInSpark(CornInfo.getString("WorldCornInfo")))
    val UsCornData=Corn(readCsvFileInSpark(CornInfo.getString("UsCornInfo")))
    val CornData=mergeOnCornAndCompute(WorldCornData,UsCornData)


    val WorldCottonData=Cotton(readCsvFileInSpark(CottonInfo.getString("WorldCottonInfo")))
    val UsCottonData=Cotton(readCsvFileInSpark(CottonInfo.getString("UsCottonInfo")))
    val CottonData=mergeOnCottonAndCompute(WorldCottonData,UsCottonData)


    //world_barley_harvest|US_barley_contribution|world_beef_slaughter|US_beef_Slaughter_contribution|world_corn_harvest|US_corn_contribution|world_cotton_harvest|US_cotton_contribution

    val otpt=BarleyData
      .join(BeefData,"ID")
      .join(CornData,"ID")
      .join(CottonData,"ID")
        .drop("ID")

    otpt.write.mode("overwrite").option("header","true").option("delimiter","|").csv("/home/slave/Desktop/src/spark.csv")


  }
}
