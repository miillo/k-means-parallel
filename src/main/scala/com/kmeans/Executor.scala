package com.kmeans

import com.kmeans.services.CentroidService
import com.kmeans.utils.ApplicationProperties
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.scalameter._
import vegas.DSL.Vegas
import vegas.sparkExt._
import vegas.spec.Spec.MarkEnums.Point
import vegas.spec.Spec.TypeEnums.{Nominal, Quantitative}

import scala.collection.mutable.ListBuffer

object Executor {
  def execute(sparkSession: SparkSession, properties: ApplicationProperties): Unit = {
    import sparkSession.implicits._

    //read source data
    var sourceDf = sparkSession.read.format("csv")
      .option("sep", properties.sourceDataDelimiter)
      .option("header", value = true)
      .load(properties.sourceDataPath)

    sourceDf.printSchema()

    if (properties.sourceDataPath.contains("diabetic")) {
      sourceDf = sourceDf
        .withColumn("class", lit(0))
        .select("time_in_hospital", "num_lab_procedures", "num_procedures", "num_medications",
          "number_outpatient", "number_emergency", "number_inpatient", "number_diagnoses", "class")
    }

    val validationDf = prepareValidationDf(sourceDf)
    //NOTE! cache() and count() operations must be performed, instead we will work on new copy every time
    validationDf.cache()
    validationDf.count()

    //dropping class attribute for clustering
    val preparedDf = sourceDf
      .drop("class")

    //draw init centroids values
    val centroidService = new CentroidService(preparedDf)
    val centroidsInit: ListBuffer[(String, ListBuffer[Float])] = centroidService.drawInitCentroids(properties)

    // create temporary dataframe for centroids updates
    val fieldsList = prepareTemporaryCentroidsResultSchema(centroidService.getMaxRowLength)
    var centroidsUpdateDf = sparkSession
      .createDataFrame(sparkSession.sparkContext.emptyRDD[Row], StructType(fieldsList))

    //create dataframe with 'clusterDecision' attribute
    val preparedDfWithClusterDec = preparedDf
      .withColumn("clusterDecision", lit(1))
      .withColumn("ID", monotonically_increasing_id())

    //measure performance time
    val performanceTime = measure {
      //repeat N iterations(from config file)
      for (_ <- 0 to properties.iterations) {
        //create dataframe with new cluster decisions
        val newClustersDf = preparedDfWithClusterDec
          .rdd
          .repartition(properties.partitionsNumber) // parallel
          .map(el => {
            var decision = ""
            var result = Double.MaxValue
            //compute point distance to each centroid and choose the least
            for (centroid <- centroidsInit) {
              var distTmp = 0.0
              // '- 2' because last two are reserved for 'clusterDecision' and ID attributes
              for (i <- 0 until el.length - 2) {
                distTmp += scala.math.pow(centroid._2(i) - el.get(i).asInstanceOf[String].toFloat, 2)
              }
              //euclidean distance
              val res = scala.math.sqrt(distTmp)
              if (res < result) {
                result = res
                decision = centroid._1
              }
            }
            //update cluster decision
            val clusterDecIndex = el.fieldIndex("clusterDecision")
            val updatedRow = el
              .toSeq
              .updated(clusterDecIndex, decision)
            Row.fromSeq(updatedRow)
          }).coalesce(1)

        //update temporary dataframe with new centroids values
        centroidsUpdateDf = sparkSession
          .createDataFrame(newClustersDf, StructType(fieldsList))
          .cache()

        //cast attributes types - RDD -> DataFrame case
        for (colName <- centroidsUpdateDf.columns) {
          if (colName == "clusterDecision") {
            centroidsUpdateDf = centroidsUpdateDf.withColumn(colName, col(colName).cast("String"))
          } else if (colName == "ID") {
            centroidsUpdateDf = centroidsUpdateDf.withColumn(colName, col(colName).cast("int"))
          } else {
            centroidsUpdateDf = centroidsUpdateDf.withColumn(colName, col(colName).cast("float"))
          }
        }

        //compute new centroid values - mean of assigned points
        for (centroid <- centroidsInit) {
          val centroidName = centroid._1
          val filtered = centroidsUpdateDf.filter($"clusterDecision" === centroidName)
          val filteredColumns = filtered.columns
          for (i <- filtered.columns.indices) {
            if (filteredColumns(i) != "clusterDecision" && filteredColumns(i) != "ID") {
              val computedMean = filtered
                .select(mean(col(filteredColumns(i))))
                .head()
                .get(0)
                .asInstanceOf[Double]
                .toFloat
              centroid._2.update(i, computedMean)
            }
          }
        }
      }
    }

    println("PERFORMANCE TIME: " + performanceTime.toString())

    //join computed values with validation dataframe and show results
    val resultDf = centroidsUpdateDf
      .join(validationDf, centroidsUpdateDf.col("ID") === validationDf.col("ID"))

    resultDf.show(150)

    //plot results:
    // iris / shapeAgg: _c0,_c1
    // diabetic: _c5,_c3 || _c6,_c3
    Vegas("Clustering", width = 600.0, height = 400.0)
      .withDataFrame(resultDf)
      .mark(Point)
      .encodeX("_c0")
      .encodeY("_c1")
      .encodeColor("clusterDecision", Nominal)
      .encodeSize("_c0", Quantitative)
      .show
  }

  /**
    * Creates dataframe for validation after clustering
    *
    * @param sourceDf source dataframe
    * @return validation dataframe
    */
  private def prepareValidationDf(sourceDf: DataFrame): DataFrame = {
    sourceDf
      .select("class")
      .withColumn("ID", monotonically_increasing_id())
  }

  /**
    * Creates dataframe schema for temporary dataframe which will hold updated centroids values
    *
    * @param maxRowLength length of row with max attributes from columns
    * @return list with struct fields used for creating temporary dataframe
    */
  private def prepareTemporaryCentroidsResultSchema(maxRowLength: Int): ListBuffer[StructField] = {
    val fieldsList = new ListBuffer[StructField]
    for (i <- 0 until maxRowLength) {
      fieldsList += StructField("_c" + i, StringType, nullable = false)
    }
    fieldsList += StructField("clusterDecision", StringType, nullable = false)
    fieldsList += StructField("ID", LongType, nullable = false)
  }
}
