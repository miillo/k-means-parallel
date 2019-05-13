package com.kmeans

import com.kmeans.utils.ApplicationProperties
import org.apache.spark.sql.{Column, Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{FloatType, StructField, StructType}

import scala.collection.mutable.ListBuffer
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
    val withoutClass = sourceDf
      .drop("class")

    // compute rows with max / min columns values for init centroid position drawing
    val maxCols: Array[Column] = withoutClass.columns.map(max)
    val minCols: Array[Column] = withoutClass.columns.map(min)
    val maxRow: Row = withoutClass.agg(maxCols.head, maxCols.tail: _*).head
    val minRow: Row = withoutClass.agg(minCols.head, minCols.tail: _*).head

    val centroidsInit: ListBuffer[ListBuffer[Float]] = new ListBuffer[ListBuffer[Float]]

    //draw initial values for centroids
    for (i <- 0 until maxRow.length) {
      for (j <- 0 until properties.kParam) {
        println("i: " + i + " @ j: " + j)
        val start = minRow.get(i).asInstanceOf[String].toFloat
        val end = maxRow.get(i).asInstanceOf[String].toFloat
        val draw = start + (end - start) * Random.nextFloat()
        if (centroidsInit.size < properties.kParam) {
          centroidsInit += ListBuffer(draw)
        } else {
          centroidsInit(j) += draw
        }
      }
    }

    // create centroids df schema
    val fieldsList = new ListBuffer[StructField]
    for (i <- 0 until maxRow.length) {
      fieldsList += StructField("_c" + i, FloatType, nullable = false)
    }

    // create centroids df
    val mappedCentroids = centroidsInit.map(el => Row.fromSeq(el))
    val centRdd = sparkSession.sparkContext.makeRDD(mappedCentroids)
    val centroidsDf = sparkSession.createDataFrame(centRdd, StructType(fieldsList))
    centroidsDf.show(20,false)

    // for each point xi ..
    val sourceDataDfPrepared = withoutClass
      .withColumn("clusterDecision", lit(1))

    //todo uh not sure but holding centroids in df may had been wrong idea... no way to iterate it as i wanted
//    sourceDataDfPrepared
//      .map(el => {
//        for (row <- centroidsDf) {
//
//        }
//      })

  }
}
