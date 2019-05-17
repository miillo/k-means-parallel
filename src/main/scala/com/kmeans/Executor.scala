package com.kmeans

import com.kmeans.utils.ApplicationProperties
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, LongType, StringType, StructField, StructType}
import org.apache.spark.sql.{Column, Row, SparkSession}

import scala.collection.mutable.ListBuffer
import scala.util.Random

object Executor {
  def execute(sparkSession: SparkSession, properties: ApplicationProperties): Unit = {
    import sparkSession.implicits._

    //read source data
    val sourceDf = sparkSession.read.format("csv")
      .option("sep", ",")
      .option("header", value = true)
      .load(properties.sourceDataPath)
    sourceDf.show(20, truncate = false)

    //preparing source dfs
    val validationDf = sourceDf
      .select("class")
      .withColumn("ID", monotonically_increasing_id())
    validationDf.cache()
    validationDf.count()

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

    // create centroids df schema
    val fieldsList = new ListBuffer[StructField]
    for (i <- 0 until maxRow.length) {
      fieldsList += StructField("_c" + i, StringType, nullable = false)
    }
    fieldsList += StructField("clusterDecision", StringType, nullable = false)
    fieldsList += StructField("ID", LongType, nullable = false)

    var pointsClustered = sparkSession
      .createDataFrame(sparkSession.sparkContext.emptyRDD[Row], StructType(fieldsList))

    val sourceDataDfPrepared = withoutClass
      .withColumn("clusterDecision", lit(1))
      .withColumn("ID", monotonically_increasing_id())

    for (_ <- 0 to properties.iterations) {
      // for each point xi ..
      var newClusters = sourceDataDfPrepared
        .rdd
        .map(el => {
          var decision = ""
          var result = Double.MaxValue
          for (centroid <- centroidsInit) {
            var distTmp = 0.0
            // - 2 because last is reserved for 'clusterDecision' and ID     //[5.1,3.5,1.4,0.2,1]
            for (i <- 0 until el.length - 2) {
              //            println("cent " + centroid._1 + " val: " + centroid._2(i) + " | el val: " +  el.get(i).asInstanceOf[String].toFloat)
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

      //preparing df for second step computations
      //cache for persisting this df for further computations
      pointsClustered = sparkSession
        .createDataFrame(newClusters, StructType(fieldsList))
        .cache()

      for (colName <- pointsClustered.columns) {
        if (colName == "clusterDecision") {
          pointsClustered = pointsClustered.withColumn(colName, col(colName).cast("String"))
        } else if (colName == "ID") {
          pointsClustered = pointsClustered.withColumn(colName, col(colName).cast("int"))
        } else {
          pointsClustered = pointsClustered.withColumn(colName, col(colName).cast("float"))
        }
      }

      //      println("XXXXXXXXXXXXXXXXXXX")
      //      pointsClustered.filter($"clusterDecision" === "k0").show(5)
      //      println(pointsClustered.filter($"clusterDecision" === "k0").count())
      //      pointsClustered.filter($"clusterDecision" === "k1").show(5)
      //      println(pointsClustered.filter($"clusterDecision" === "k1").count())
      //      //TODO Tutaj przed petla count wypisuje np. 54 dla k2
      //      pointsClustered.filter($"clusterDecision" === "k2").show(5)
      //      println(pointsClustered.filter($"clusterDecision" === "k2").count())

      //for each cluster j ..
      println(centroidsInit)
      for (centroid <- centroidsInit) {
        println("Centroid type: " + centroid.getClass + " | centroid name type: " + centroid._1.getClass)
        println("Centroid: " + centroid)
        val centroidName = centroid._1
        //TODO prawdopodbnie tutaj filtrowanie sie psuje, ale dlaczego? wartosc centroidName jest ok - probowalem tez z fun. 'like'
        val filtered = pointsClustered.filter($"clusterDecision" === centroidName)
        filtered.show(20)
        //TODO natomiast tutaj wypisuje inna wartosc niz ta przed petla mimo ze to ta sama instancja?
        println(filtered.count())
        val filteredColumns = filtered.columns
        for (i <- filtered.columns.indices) {
          if (filteredColumns(i) != "clusterDecision" && filteredColumns(i) != "ID") {
            //compute mean
            val computedMean = filtered
              .select(mean(col(filteredColumns(i))))
              .head()
              .get(0)
              .asInstanceOf[Double]
              .toFloat

            //          if (!meanValues.contains(centroidName)) {
            //            meanValues += (centroidName -> ListBuffer(computedMean))
            //          } else {
            //            meanValues(centroidName) += computedMean
            //          }

            //TODO ta linijka powoduje takie zaklocenia
            centroid._2.update(i, computedMean)
          }
        }
      }

      //    println(meanValues)
      println(centroidsInit)
    }

    pointsClustered
      .join(validationDf, pointsClustered.col("ID") === validationDf.col("ID"))
      .show(150)
  }
}
