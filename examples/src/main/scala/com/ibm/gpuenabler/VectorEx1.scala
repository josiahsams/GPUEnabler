package com.ibm.gpuenabler

import com.ibm.gpuenabler.CUDARDDImplicits._
import com.ibm.gpuenabler.CUDADSImplicits._
import org.apache.spark.SparkEnv
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.SparkConf

import org.apache.spark.mllib.linalg.Vectors

case class VectorDataPoint(x: org.apache.spark.mllib.linalg.Vector)

object VectorEx1 {
  def timeit(msg: String, code: => Any): Any ={
    val now1 = System.nanoTime
    code
    val ms1 = (System.nanoTime - now1) / 1000000
    println("%s Elapsed time: %d ms".format(msg, ms1))
  }

  def main(args : Array[String]): Unit = {

    val masterURL = if (args.length > 0) args(0) else "local[*]"

    val conf = new SparkConf(false).set("spark.executor.memory", "20g")
    val spark = SparkSession.builder().master(masterURL).appName("test").config(conf).getOrCreate()
    import spark.implicits._

    val sc = spark.sparkContext

    val ptxURL = this.getClass.getResource("/GpuEnablerExamples.ptx")
    val ptxURL1 = "/GpuEnablerExamples.ptx"

    val dsmapFunction = DSCUDAFunction(
      "multiplyBy2D",
      Array("x"),
      Array("x"),
      ptxURL1)

    val inputDS = spark.range(0, 10, 1, 1)
	.map(x => VectorDataPoint(Vectors.dense(Array(1.1, 2.2, 3.3, 4.4, 5.5)))).cache

    inputDS.collect().foreach(println)

    inputDS.printSchema()
	
    inputDS.mapExtFunc((x : VectorDataPoint) => x, 
	dsmapFunction, 
	Array(5),
	outputArraySizes = Array(5)).collect().foreach(println)

  }
}
