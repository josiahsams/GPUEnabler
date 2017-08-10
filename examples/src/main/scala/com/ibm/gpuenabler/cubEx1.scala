package com.ibm.gpuenabler

import com.ibm.gpuenabler.CUDADSImplicits._
import org.apache.spark.SparkEnv
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.SparkConf

object cubEx1 {
  def timeit(msg: String, code: => Any): Any ={
    val now1 = System.nanoTime
    code
    val ms1 = (System.nanoTime - now1) / 1000000
    println("%s Elapsed time: %d ms".format(msg, ms1))
  }

  def main(args : Array[String]): Unit = {

    val masterURL = if (args.length > 0) args(0) else "local[*]"
    val num:Int = if (args.length > 1) args(1).toInt else 10
    val part:Int = if (args.length > 2) args(2).toInt else 1

    val conf = new SparkConf(false).set("spark.executor.memory", "20g")
    val spark = SparkSession.builder().master(masterURL).appName("test").config(conf).getOrCreate()
    import spark.implicits._

    val sc = spark.sparkContext

    val ptxURL1 = "/cubKernel.ptx"

    val threads = 1024
    val blocks = (((num/part) + threads- 1) / threads)

    val sched_threads = 1024/16; // Tile Size

    val dimensions = (size: Long, stage: Int) => stage match {
      case 0 => (blocks, sched_threads, 1, 1, 1, 1)
    }

    val gpuParams = gpuParameters(dimensions)

    val dsreduceFunction = DSCUDAFunction(
      "invokeBlockSumKernel",
      Array("value"),
      Array("value"),
      ptxURL1, Some((size: Long) => 1),
        Some(gpuParams), outputSize=Some(1))

    val inputDS = spark.range(0, num, 1, part).cache

    inputDS.collect()

    println(s"Generated $num datapoints")
	
    val output = inputDS.reduceExtFunc(_ + _,
        dsreduceFunction)

    val expResult = inputDS.reduce(_ + _)

    println("Output is " + output)
    println("Expected Output is " + expResult)
  }
}
