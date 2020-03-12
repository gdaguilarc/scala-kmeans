 /*
  * 
  * 
  * COURSE: Data-Intensive Programming 
  * TEAM: Pedrito Sola ;)
  * UNIVERSITY: Tampere University
  * 
  * 
  * 
  */

package assignment19

import org.apache.spark.SparkConf
import org.apache.spark.sql.functions.{window, column, desc, col}


import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.Column
import org.apache.spark.sql.Row
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.{ArrayType, StringType, StructField, IntegerType}
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.{count, sum, min, max, asc, desc, udf, to_date, avg, when, lit}

import org.apache.spark.sql.functions.explode
import org.apache.spark.sql.functions.array
import org.apache.spark.sql.SparkSession

import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}




import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.clustering.{KMeans, KMeansSummary}

import java.io.{PrintWriter, File}


//import java.lang.Thread
import sys.process._


import org.apache.log4j.Logger
import org.apache.log4j.Level
import scala.collection.immutable.Range



import org.apache.spark.ml.evaluation.Evaluator

object assignment  {
  // Suppress the log messages:
  Logger.getLogger("org").setLevel(Level.OFF)
                       
  
  val spark = SparkSession.builder().appName("assigment").config("spark.driver.host", "localhost").master("local").getOrCreate()
  
  val excluColumns= ("LABEL")
  val dataK5D2 =  spark.read
                       .option("sep", ",")
                       .option("header","true")
                       .option("inferSchema", "true")
                       .csv("data/dataK5D2.csv")
                       .drop(excluColumns)

  val dataK5D3 =  spark.read
                       .option("sep", ",")
                       .option("header","true")
                       .option("inferSchema", "true")
                       .csv("data/dataK5D3.csv")
                       .drop(excluColumns)
                       
   val dataK5D2WithLabels = spark.read
                       .option("sep", ",")
                       .option("header","true")
                       .option("inferSchema", "true")
                       .csv("data/dataK5D2.csv")
  
  /*
   * This function clusters 2 dimensional data in a given number of groups (k) and returns the mean of each group
   * 
   * @Param df Raw dataFrame 
   * @Param k The number of means
   * @Return An Array of the means (a,b)
   * 
   * */
  def task1(df: DataFrame, k: Int): Array[(Double, Double)] = {
    
    // Pipeline for training
    val vectorAssembler = new VectorAssembler()
    .setInputCols(Array("a", "b"))
    .setOutputCol("features")
   
    val transformationPipeline = new Pipeline().setStages(Array(vectorAssembler))
    
    val pipeLine = transformationPipeline.fit(df)
    val transformedTraining = pipeLine.transform(df)
    
    // Kmeans model fitting to the data
    val kmeans = new KMeans().setK(k).setSeed(1L)
    val kmModel= kmeans.fit(transformedTraining)
    
    val centers = kmModel.clusterCenters.map(x => (x(0), x(1)))
    return centers
  }
  
   /*
   * This function clusters 3 dimensional data in a given number of groups (k) and returns the mean of each group
   * 
   * @Param df Raw dataFrame 
   * @Param k The number of means
   * @Return An Array of the means (a,b,c)
   * 
   * */
  def task2(df: DataFrame, k: Int): Array[(Double, Double, Double)] = {
    
    // Pipeline for training
    val vectorAssembler = new VectorAssembler()
    .setInputCols(Array("a", "b", "c"))
    .setOutputCol("features")
   
    val transformationPipeline = new Pipeline().setStages(Array(vectorAssembler))
    
    val pipeLine = transformationPipeline.fit(df)
    val transformedTraining = pipeLine.transform(df)
    
    // Kmeans model fitting to the data
    val kmeans = new KMeans().setK(k).setSeed(1L)
    val kmModel= kmeans.fit(transformedTraining)
    
    val centers = kmModel.clusterCenters.map(x => (x(0), x(1), x(2)))
    return centers
  }

  /*
   * This function gets the means of the data grouped by the label, adds a 3rd dimension to group Fatal and Ok in
   * separate groups and get the mean of each one
   * 
   * @Param df Raw dataFrame 
   * @Param k The number of means
   * @Return An Array of the means (a,b)
   * 
   * */
  def task3(df: DataFrame, k: Int): Array[(Double, Double)] = {
    
    //  Add 3rd dimension based on label Ok = 0, Fatal = 10
    val parsedDF = df.withColumn("diagnosis", when(col("label").contains("Ok"), 0).otherwise(10))
    
    // Pipeline for training
    val vectorAssembler = new VectorAssembler()
    .setInputCols(Array("a", "b", "diagnosis"))
    .setOutputCol("features")
   
    val transformationPipeline = new Pipeline().setStages(Array(vectorAssembler))
    
    val pipeLine = transformationPipeline.fit(parsedDF)
    val transformedTraining = pipeLine.transform(parsedDF)
    
    // Kmeans model fitting to the data
    val kmeans = new KMeans().setK(2).setSeed(1L)
    val kmModel= kmeans
    .setFeaturesCol("features")
    .setPredictionCol("prediction")
    .fit(transformedTraining)
    
    val centers = kmModel.clusterCenters.map(x => (x(0), x(1)))
    return centers
  }
  
  /*
   * This function is used to apply the kmeans model for a range of k
   * 
   * @Param df The transformed pipeline
   * @Param n The current index of the function
   * @Param high Limit used for the base case
   * @Return An Array of pairs (k, cost) between a range
   * 
   * */
  def recursiveTask4(df: DataFrame, n: Int, high: Int): Array[(Int, Double)] = {
   
    // Kmeans model fitting to the data
    val kmeans = new KMeans().setK(n).setSeed(1L)
    val kmModel= kmeans.fit(df)
   
    // Compute the cost
    val wssse = kmModel.computeCost(df)
   
    // Base Case
    if (n == high){
      return Array((n, wssse))
    }else {
      return Array((n, wssse)) ++: recursiveTask4(df, n + 1, high)
    }

  }
  
  /*
   * This function is used to apply the kmeans model for a range of k, for this it triggers the recursiveTask4, 
   * because in SCALA we cannot reasign a variable and we want that the program can be parallelized
   * 
   * @Param df Raw dataFrame 
   * @Param low First k to try
   * @Param high Limit of ks 
   * @Return An Array of pairs (k, cost) between a range
   * 
   * */
  def task4(df: DataFrame, low: Int, high: Int): Array[(Int, Double)]  = {
    
    // Pipeline for training
    val vectorAssembler = new VectorAssembler()
    .setInputCols(Array("a", "b"))
    .setOutputCol("features")
   
    val transformationPipeline = new Pipeline().setStages(Array(vectorAssembler))
    
    val pipeLine = transformationPipeline.fit(df)
    val transformedTraining = pipeLine.transform(df)
       
    val results = recursiveTask4(transformedTraining, low, high)
   
    return results
  }
    
}


