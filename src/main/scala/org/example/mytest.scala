package org.example

import java.io.{BufferedWriter, File, FileNotFoundException, FileOutputStream, FileWriter}

import com.typesafe.config.ConfigFactory
import javax.jws.soap.SOAPBinding.Use
import org.example.Utils.helperClass._

import scala.io.Source

object mytest {


  def main(args: Array[String]): Unit = {


    var outputForCorn : String=""
    var outputForCotton : String=""
    var cnt=0
    // Load the propertis file to get the resource
    val config = ConfigFactory.load("application.conf").getConfig("resourceFile")
    val CornInfo = config.getConfig("Corn")
    val CottonInfo = config.getConfig("Cotton")
    val BarleyInfo = config.getConfig("Barley")
    val BeefInfo = config.getConfig("Beef")
    val WheetInfo = config.getConfig("Wheet")

   /* val data= scala.io.Source.fromFile(BeefInfo.getString("UsBeefInfo"))
    val data1= scala.io.Source.fromFile(BeefInfo.getString("WorldBeefInfo"))

   val o=data.getLines().drop(1).map(p=>p.split(","))
    val j=data1.getLines().drop(1).map(p=>p.split(","))
    val l=o.map(p=>
      (p(0),matchPattern(p(1).trim()))).toMap
    print(manOf(l))

    println(j.map(p=>
      (p(0),matchPattern(p(1).trim()))).toMap) */

    //get the Barley data Info
    val UsBarleyData=selectFields(removeHeaderandSplit(readFile(BarleyInfo.getString("UsBarleyInfo"))))
    val WorldBarleyData=selectFields(removeHeaderandSplit(readFile(BarleyInfo.getString("WorldBarleyInfo"))))

    //get the Beef data Info
    val UsBeefData=selectFields(removeHeaderandSplit(readFile(BeefInfo.getString("UsBeefInfo"))))
    val WorldBeefData=selectFields(removeHeaderandSplit(readFile(BeefInfo.getString("WorldBeefInfo"))))

    //get the UserCorn Data
    val WorldCornData=selectFields(removeHeaderandSplit(readFile(CornInfo.getString("WorldCornInfo"))))
    val UsCornData=selectFields(removeHeaderandSplit(readFile(CornInfo.getString("UsCornInfo"))))

    val UsCottonData=selectFields(removeHeaderandSplit(readFile(CottonInfo.getString("UsCottonInfo"))))
    val WorldCottonData=selectFields(removeHeaderandSplit(readFile(CottonInfo.getString("WorldCottonInfo"))))


    // Get the key and value pair

    val Barley=keyJoin(WorldBarleyData,UsBarleyData)
    val Beef=keyJoin(WorldBeefData,UsBeefData)
    val Corn=keyJoin(WorldCornData,UsCornData)
    val Cotton=keyJoin(WorldCottonData,UsCottonData)


    val rsltForBarley=Barley.map(p=> (p._1,p._2._1,((p._2._2).toString.toDouble/(p._2._1).toString.toDouble)*100))
    val rsltForBeef=Beef.map(p=> (p._1,p._2._1,((p._2._2).toString.toDouble/(p._2._1).toString.toDouble)*100))
    val rsltForCorn=Corn.map(p=> (p._1,p._2._1,((p._2._2).toString.toDouble/(p._2._1).toString.toDouble)*100))
    val rsltForCotton=Cotton.map(p=> (p._1,p._2._1,((p._2._2).toString.toDouble/(p._2._1).toString.toDouble)*100))


  // Write to a file, currently dealing with 4 files.

    val file="/home/slave/Desktop/src/file.txt"
    val writer = new BufferedWriter(new FileWriter(file))
    writer.write("Year|world_Barley_harvest|usa_Barley_contribution%|Year|world_Beef_Slaughter|usa_Beef_Slaugter_contribution%|Year|world_Corn_harvest|usa_Corn_contribution%|world_Cotton_harvest|usa_Cotton_contribution%\n")
    // group on keys and print
    List(rsltForBarley,rsltForBeef,rsltForCorn,rsltForCotton).flatten.groupBy(_._1).toSeq.sortBy(_._1).map(p=> {
      p._2.foreach(d=> {

          if (cnt< 4 ) {
            writer.write(s"""${d._1}|${d._2}|${infinityChk(d._3)}|""")
            cnt = cnt + 1
          }
          else {
            writer.newLine()
            writer.write(s"""${d._1}|${d._2}|${infinityChk(d._3)}|""")
            cnt=1
          }


      })

    })
    writer.close()




  }

}
