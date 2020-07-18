package org.example.Utils

import scala.io._
import scala.collection._
object helperClass {

  def infinityChk(d: Double): Double ={
    val p=d.toString()

    //val scalaDOUBLE : scala.util.matching.Regex = DOUBLE.r
    p match {
      case "Infinity"   => 0.0
      case _            => d
    }

    }

  def matchPattern(x: Any): Any = x match {
    case "--" => 0
    case _ => x
  }

  def readFile(file: String): BufferedSource ={
    val data= scala.io.Source.fromFile(file)
    return data
  }

  def removeHeaderandSplit(parse: BufferedSource): Iterator[Array[java.lang.String]]={
    return parse.getLines()
      .drop(1)
      .map(p=>p.split(","))
  }

  def selectFields(recordSelect: Iterator[Array[java.lang.String]]): immutable.Map[java.lang.String, Any] ={
    return recordSelect.map(p=>
      (p(0),matchPattern(p(1).trim()).toString.toDouble)).toMap
  }

  // this func combine the value based on key
  def keyJoin[K, v1, v2](m1: Map[K, v1], m2: Map[K, v2]): Map[K, (v1, v2)] = {
    m1.flatMap{ case (k, a) =>
      m2.get(k).map(b => Map((k, (a, b))))
        .getOrElse(Map.empty[K, (v1, v2)])
    }
  }


}
