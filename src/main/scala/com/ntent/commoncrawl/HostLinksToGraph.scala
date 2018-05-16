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

  def convertArrayToArgumentMaps(args: Array[String]) : Map[String, String] =  {
    import scala.collection.mutable
    val result = mutable.Map[String, String]()
    try {
      result("InputParquet")   = args(0)
      result("EdgesOutput")    = args(1)
      result("VerticesOutput") = args(2)
      result("ValidateHosts")  = args(3)
      result("SaveAsText")     = args(4)
      result("NumPartitions")  = args(5)
      result("VertexIDs")      = args(6)
    } catch {
      case _: IndexOutOfBoundsException => {}
    }
    result.toMap
  }

  def main(args: Array[String]): Unit = {
    val jobName: String = this.getClass.getName

    val fs = FileSystem.get(new Configuration())
    val conf = SparkSession.builder().appName(jobName)
    val spark = conf.getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("warn")

    val mapOfArguments = convertArrayToArgumentMaps(args);

    processParquetFile(spark, mapOfArguments.get("InputParquet"),
      mapOfArguments.get("EdgesOutput"), mapOfArguments.get("VerticesOutput"),
      mapOfArguments.get("ValidateHosts"), mapOfArguments.get("SaveAsText"),
      mapOfArguments.get("NumPartitions"), mapOfArguments.get("VertexIDs"))
  }

  def processParquetFile(spark: SparkSession, inputParquet: Option[String], edgesOutput: Option[String],
                         verticesOutput: Option[String], validateHosts: Option[String],
                         saveAsText: Option[String], numPartitions: Option[String], vertexIDs: Option[String]) {
    import spark.implicits._

    if (inputParquet.isEmpty || edgesOutput.isEmpty || verticesOutput.isEmpty) return

    val edges = spark.read.parquet(inputParquet.get).dropDuplicates("s", "t")

    val mustSaveAsText = saveAsText.isEmpty == false && saveAsText.get.toBoolean == true
    val ids = if (vertexIDs.isEmpty) verticesAssignIds(edges, spark, verticesOutput.get, mustSaveAsText, validateHosts, numPartitions)
    else spark.read.parquet(vertexIDs.get)

    val edgesSJoin = edges.join(ids, $"s" === $"name")
    val edgesSID = edgesSJoin.select($"id".as("s"), $"t")
    val edgesTJoin = edgesSID.join(ids, $"t" === $"name")
    val edgesTID = edgesTJoin.select($"s", $"id".as("t"))
    val sortedEdges = edgesTID.sort($"s")

    if (mustSaveAsText) {
      saveAsTxtZip(sortedEdges, edgesOutput.get + "/zip/")
    }
    saveAsParquet(sortedEdges, edgesOutput.get + "/parquet/")
  }

  def verticesAssignIds(edges: DataFrame, spark: SparkSession, verticesOutput: String,
                        mustSaveAsText: Boolean, validateHosts: Option[String], numPartitions: Option[String]) = {
    import spark.implicits._

    val source = edges.select($"s".as("name"))
    val target = edges.select($"t".as("name"))

    val ids = source.union(target).distinct

    val mustValidateHosts = validateHosts.isEmpty == false && validateHosts.get.toBoolean == true
    val validIds = if (mustValidateHosts) {
      val isValidUDF = udf(reverseHostIsValid _)
      ids.filter(isValidUDF($"name"))
    } else ids

    val idsRDD = validIds.select($"name").rdd
      .map(_.getString(0))
      .sortBy(identity, ascending=true, numPartitions = numPartitions.getOrElse("100").toInt)
      .zipWithIndex()
    val idsDF = idsRDD.toDF("name", "id").select($"id", $"name")

    if (mustSaveAsText) {
      saveAsTxtZip(idsDF, verticesOutput + "/zip/")
    }
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
