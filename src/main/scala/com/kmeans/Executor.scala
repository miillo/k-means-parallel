package com.kmeans

import com.kmeans.utils.ApplicationProperties
import org.apache.spark.sql.{Column, Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{FloatType, StringType, StructField, StructType}

import scala.collection.mutable.ListBuffer
import scala.util.Random

object Executor {
  def execute(sparkSession: SparkSession, properties: ApplicationProperties): Unit = {

    //read source data
    val sourceDf = sparkSession.read.format("csv")
      .option("sep", ",")
      .option("header", value = true)
      .load("DataSets/iris.data")
    sourceDf.show(20, truncate = false)

    //test area
    //*************************
//    import sparkSession.implicits._
//    val df = Seq((1, "one"), (2, "two"), (3, "three")).toDF()
//    df.show(10)
//
//    val col = df.col("_2")
//    val dropped = df.drop("_2")
//    val dropclone = sparkSession.createDataFrame(dropped.rdd, dropped.schema)
//    dropclone.show(10)
//    println(col)
//
//    dropclone.withColumn("_2", col).show(10)
    //*************************

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

    val centroidsInit: ListBuffer[(String, ListBuffer[Float])] = new ListBuffer[(String, ListBuffer[Float])]

    //draw initial values for centroids
    for (i <- 0 until maxRow.length) {
      for (j <- 0 until properties.kParam) {
        //        println("i: " + i + " @ j: " + j)
        val start = minRow.get(i).asInstanceOf[String].toFloat
        val end = maxRow.get(i).asInstanceOf[String].toFloat
        val draw = start + (end - start) * Random.nextFloat()
        if (centroidsInit.size < properties.kParam) {
          centroidsInit += Tuple2("k" + j, ListBuffer(draw))
        } else {
          centroidsInit(j)._2 += draw
        }
      }
    }

    /**
      * by now transforming init centroids to df is suspended
      */
    //    // create centroids df schema
    val fieldsList = new ListBuffer[StructField]
    for (i <- 0 until maxRow.length) {
      fieldsList += StructField("_c" + i, StringType, nullable = false)
    }
    fieldsList += StructField("clusterDecision", StringType, nullable = false)
    //
    //    // create centroids df
    //    val mappedCentroids = centroidsInit.map(el => Row.fromSeq(el))
    //    val centRdd = sparkSession.sparkContext.makeRDD(mappedCentroids)
    //    val centroidsDf = sparkSession.createDataFrame(centRdd, StructType(fieldsList))
    //    centroidsDf.show(20,false)

    val sourceDataDfPrepared = withoutClass
      .withColumn("clusterDecision", lit(1))

    // for each point xi ..
    var newClusters = sourceDataDfPrepared
      .rdd
      .map(el => {
        var decision = ""
        var result = Double.MaxValue
        for (centroid <- centroidsInit) {
          var distTmp = 0.0
          // - 1 because last is reserved for 'clusterDecision'
          for (i <- 0 until el.length - 1) {
            //compute euclidean distance
            distTmp += scala.math.pow(centroid._2(i) - el.get(i).asInstanceOf[String].toFloat, 2)
          }
          val res = scala.math.sqrt(distTmp)
          if (res < result) {
            result = res
            decision = centroid._1
          }
        }
        val clusterDecIndex = el.fieldIndex("clusterDecision")
        val updatedRow = el
          .toSeq
          .updated(clusterDecIndex, decision)
        Row.fromSeq(updatedRow)
      })

    var testDf = sparkSession
      .createDataFrame(newClusters, StructType(fieldsList))

    testDf.show(10)
    testDf.printSchema()

    for (colName <- testDf.columns) {
      if (colName == "clusterDecision") {
        println("HIHI")
        testDf = testDf.withColumn(colName, col(colName).cast("String"))
      } else {
        testDf = testDf.withColumn(colName, col(colName).cast("float"))
      }
    }

    testDf.show(10)
    testDf.printSchema()
    //
//    println("??????????")
//    val decCol = testDf.col("clusterDecision")
//    //is there event antyhing ?!
//    println(decCol)
//    val omg = testDf.select(testDf.columns.init.map(c => col(c).cast(FloatType)): _*)
//    val omgomg = omg.withColumn("clusterDecision", decCol)
//
//    omgomg.printSchema()
//    omgomg.show(20)
//
//    for (centroid <- centroidsInit) {
//      val centroidName = centroid._1
//
//    }
//    println("WOBO2")
//    newClusters.take(20).foreach(println)
  }
}
