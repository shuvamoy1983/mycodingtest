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


    val WorldCottonData=Cotton(readCsvFileInSpark(CottonInfo.getString("UsCottonInfo")))
    val UsCottonData=Cotton(readCsvFileInSpark(CottonInfo.getString("WorldCottonInfo")))


    val p=BeefData
    p.show()





  }
}
