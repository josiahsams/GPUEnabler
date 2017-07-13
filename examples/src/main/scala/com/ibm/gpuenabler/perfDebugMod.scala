package com.ibm.gpuenabler

import com.ibm.gpuenabler.CUDARDDImplicits._
import com.ibm.gpuenabler.CUDADSImplicits._

import jcuda.Pointer
import org.apache.spark.SparkEnv
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf


trait GPUFunc {
  def run(args: Array[AnyRef])
}

class malMul extends GPUFunc{
  def run(args: Array[AnyRef]): Unit = {

    println("No of args " + args.length)
    args(0) match {
      case ptr if ptr.isInstanceOf[Pointer] => println("Pointer 1  is called")
      case _ => println("Incorrect Args")
    }
    args(1) match {
      case ptr if ptr.isInstanceOf[Pointer] => println("Pointer 2 is called")
      case _ => println("Incorrect Argument")
    }
  }
}

object perfDebugMod {
  def timeit(msg: String, code: => Any): Any ={
    val now1 = System.nanoTime
    code
    val ms1 = (System.nanoTime - now1) / 1000000
    println("%s Elapsed time: %d ms".format(msg, ms1))
  }

  def main(args : Array[String]): Unit = {

    val masterURL = if (args.length > 0) args(0) else "local[*]"
    val n: Long = if (args.length > 1) args(1).toLong else 10L
    // val n: Long = if (args.length > 1) args(1).toLong else 1000000L
    // val part = if (args.length > 2) args(2).toInt else 16
    val part = if (args.length > 2) args(2).toInt else 1

    val conf = new SparkConf(false).set("spark.executor.memory", "20g")
    val spark = SparkSession.builder().master(masterURL).appName("test").config(conf).getOrCreate()
    import spark.implicits._

    val sc = spark.sparkContext
    val ptxURL1 = ""

    val dsmapFunction = DSCUDAFunction(
      "com.ibm.gpuenabler.malMul.run",
      Array("value"),
      Array("value"),
      ptxURL1)

    val rd = spark.range(1, n+1, 1, part).cache()
    rd.count()

    val data = rd.cacheGpu(true)
    // Load the data to GPU
    // data.loadGpu()

    timeit("DS: All cached", {
      val mapDS = data.mapExtFunc(2 * _, dsmapFunction).cacheGpu()
      mapDS.collect().foreach(println)
      // val output = mapDS.reduceExtFunc(_ + _, dsreduceFunction)
      //mapDS.unCacheGpu()
      // println("Output is " + output)
    })
  }
}


