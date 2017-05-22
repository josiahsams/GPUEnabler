package com.ibm.gpuenabler

import com.ibm.gpuenabler.CUDARDDImplicits._

import org.apache.spark.SparkEnv
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf

object Demo1 {
  def main(args : Array[String]): Unit = {

    val masterURL = if (args.length > 0) args(0) else "local[*]"
    val n: Long = if (args.length > 1) args(1).toLong else 1000000L
    val part = if (args.length > 2) args(2).toInt else 1

    val conf = new SparkConf(false).set("logLevel", "1")
    val spark = SparkSession.builder().master(masterURL).appName("test").config(conf).getOrCreate()
    import spark.implicits._

    val sc = spark.sparkContext

    val ptxURL = this.getClass.getResource("/GpuEnablerExamples.ptx")
    val ptxURL1 = "/GpuEnablerExamples.ptx"
    val mapFunction = new CUDAFunction(
      "multiplyBy2",
      Array("this"),
      Array("this"),
      ptxURL)

    val dimensions = (size: Long, stage: Int) => stage match {
      case 0 => (64, 256)
      case 1 => (1, 1)
    }
    val reduceFunction = new CUDAFunction(
      "suml",
      Array("this"),
      Array("this"),
      ptxURL,
      Seq(),
      Some((size: Long) => 2),
      Some(dimensions))

    val dataRDD = sc.parallelize(1 to n.toInt, 80).map(_.toLong).cache()
    dataRDD.count()
    val rdata = dataRDD.repartition(part).cache()
    rdata.count()
    println(s"Data Vector of size ($n x 1) is created")
    val now = System.nanoTime
    
    println("Multiple each item in the vector with 2 and sum up all the elements")
    println()

    var output: Long = rdata.mapExtFunc((x: Long) => 2 * x, mapFunction)
      .reduceExtFunc((x: Long, y: Long) => x + y, reduceFunction)

    val ms = (System.nanoTime - now) / 1000000
    println("GPU Elapsed time: %d ms".format(ms))
    
    println("Output is " + output)

   }

}


