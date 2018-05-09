package com.ntent.commoncrawl

import org.apache.commons.validator.routines.DomainValidator
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Created by soneraltin on 4/24/18.
  */
object HostLinksToGraph {

  def main(args: Array[String]): Unit = {
    val jobName: String = this.getClass.getName

    val fs = FileSystem.get(new Configuration())
    val conf = SparkSession.builder().appName(jobName)
    val sparkSession = conf.getOrCreate()
    val sc = sparkSession.sparkContext
    sc.setLogLevel("warn")

    val inputParquetPath = ""
    val edgesOutput = ""
    val verticesOutput = ""

    processParquetFile(sparkSession, inputParquetPath, edgesOutput, verticesOutput)
  }

  def processParquetFile(spark: SparkSession, inputParquetPath: String, edgesOutput: String, verticesOutput: String) = {
    import spark.implicits._

    val edges = spark.read.parquet(inputParquetPath).dropDuplicates("s", "t")
    val ids = verticesAssignIds(edges, spark, verticesOutput)

    val edgesSJoin = edges.join(ids, $"s" === $"name")
    val edgesSID = edgesSJoin.select($"id".as("s"), $"t")
    val edgesTJoin = edgesSID.join(ids, $"t" === $"name")
    val edgesTID = edgesTJoin.select($"s", $"id".as("t"))
    val sortedEdges = edgesTID.sort($"s")

    saveAsTxtZip(sortedEdges, edgesOutput + "/zip/")
    saveAsParquet(sortedEdges, edgesOutput + "/parquet/")
  }

  def verticesAssignIds(edges: DataFrame, spark: SparkSession, verticesOutput: String) = {
    import spark.implicits._

    val source = edges.select($"s".as("name"))
    val target = edges.select($"t".as("name"))

    val ids = source.union(target).distinct
    val isValidUDF = udf(reverseHostIsValid _)
    val validIds = ids.filter(isValidUDF($"name"))
    val idsRDD = validIds.select($"name").rdd
      .map(_.getString(0))
      .sortBy(identity, ascending=true, numPartitions = 100)
      .zipWithIndex()
    val idsDF = idsRDD.toDF("name", "id").select($"id", $"name")

    saveAsTxtZip(idsDF, verticesOutput + "/zip/")
    saveAsParquet(idsDF, verticesOutput + "/parquet/")
    idsDF
  }

  def reverseHostIsValid(reverseHost: String): Boolean = {
    if (reverseHost.contains('.') == false) {
      false
    } else {
      DomainValidator.getInstance().isValidTld(reverseHost.split('.')(0))
    }
  }

  def saveAsParquet(df: DataFrame, address: String) = {
    val startTime = System.currentTimeMillis()
    println("Saving to " + address)
    val path = new Path(address)
    FileSystem.get(new Configuration()).delete(path, true)
    df.write.parquet(address)
    println("Saved to " + address + " in (seconds) " + (System.currentTimeMillis() - startTime) / 1000)
  }

  def saveAsTxtZip(df: DataFrame, address: String) = {
    val startTime = System.currentTimeMillis()
    println("Saving to " + address)
    val path = new Path(address)
    FileSystem.get(new Configuration()).delete(path, true)
    df.write.format("csv").option("compression", "gzip").option("delimiter", "\t").save(address)
    println("Saved to " + address + " in (seconds) " + (System.currentTimeMillis() - startTime) / 1000)
  }
}
