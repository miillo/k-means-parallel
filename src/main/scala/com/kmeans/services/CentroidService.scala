package com.kmeans.services

import com.kmeans.utils.ApplicationProperties
import org.apache.spark.sql.functions.{max, min}
import org.apache.spark.sql.{Column, DataFrame, Row}

import scala.collection.mutable.ListBuffer
import scala.util.Random

class CentroidService(preparedDf: DataFrame) {
  private val maxRow: Row = createRowWithMaxColValues()
  private val minRow: Row = createRowWithMinColValues()

  def getMaxRowLength: Int = maxRow.length

  /**
    * Draws init values for centroid based on max / min values for each column
    *
    * @param properties object with application properties
    */
  def drawInitCentroids(properties: ApplicationProperties): ListBuffer[(String, ListBuffer[Float])] = {
    val centroidsInit: ListBuffer[(String, ListBuffer[Float])] = new ListBuffer[(String, ListBuffer[Float])]

    for (i <- 0 until maxRow.length) {
      for (j <- 0 until properties.kParam) {
        val start = minRow.get(i).asInstanceOf[String].toFloat
        val end = maxRow.get(i).asInstanceOf[String].toFloat
        val draw = start + (end - start) * Random.nextFloat() //draw value within range
        if (centroidsInit.size < properties.kParam) {
          centroidsInit += Tuple2("k" + j, ListBuffer(draw))
        } else {
          centroidsInit(j)._2 += draw
        }
      }
    }

    centroidsInit
  }

  /**
    * Gets row instance with max values from each column
    *
    * @return row with max values
    */
  private def createRowWithMaxColValues(): Row = {
    val maxCols: Array[Column] = preparedDf.columns.map(max)
    preparedDf
      .agg(maxCols.head, maxCols.tail: _*)
      .head
  }

  /**
    * Gets row instance with min values from each column
    *
    * @return row with min values
    */
  private def createRowWithMinColValues(): Row = {
    val minCols: Array[Column] = preparedDf.columns.map(min)
    preparedDf.agg(minCols.head, minCols.tail: _*).head
  }
}
