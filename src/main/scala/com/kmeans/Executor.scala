package com.kmeans

import com.kmeans.utils.ApplicationProperties
import org.apache.spark.sql.{Column, DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions._

import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, ListBuffer}
import scala.math.Integral.Implicits.infixIntegralOps
import scala.util.Random

object Executor {
  def execute(sparkSession: SparkSession, properties: ApplicationProperties): Unit = {
    import sparkSession.implicits._

    //read source data
    val sourceDf = sparkSession.read.format("csv")
      .option("sep", ",")
      .option("header", value = true)
      .load("DataSets/iris.data")
    sourceDf.show(20, truncate = false)

    //save class column
    val classCol = sourceDf.col("class")
    //delete class column
    val withoutClass = sourceDf.drop("class")

    val maxCols: Array[Column] = withoutClass.columns.map(max)
    val minCols: Array[Column] = withoutClass.columns.map(min)
    val maxRow: Row = withoutClass.agg(maxCols.head, maxCols.tail: _*).head
    val minRow: Row = withoutClass.agg(minCols.head, minCols.tail: _*).head

    //[
    //(...)
    //]
    var centroidsInit: ListBuffer[ArrayBuffer[Float]] = new mutable.ListBuffer[ArrayBuffer[Float]]

    println(maxRow.length)
    println("k: " + properties.kParam)

    //draw initial values for centroids
    for (i <- 0 until maxRow.length) {
      for (j <- 0 until properties.kParam) {
        println("i: " + i + " @ j: " + j)
        val start = minRow.get(i).asInstanceOf[String].toFloat
        val end = maxRow.get(i).asInstanceOf[String].toFloat
        val draw = start + (end - start) * Random.nextFloat()
        if (centroidsInit.size < properties.kParam) {
          centroidsInit += ArrayBuffer(draw)
        } else {
          centroidsInit(j) += draw
        }
      }
    }

    println(centroidsInit.size)
    centroidsInit.foreach(el => println(el))


 //   for (i <- 0 until centroidsInit.length) {
 //     centroidsInit(i).toDF(withoutClass.columns: _*)
  //  }
//cast generated values to df TODO
  centroidsInit(1).toDF().show(10, false)
  }
}
