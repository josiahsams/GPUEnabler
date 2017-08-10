package com.ibm.gpuenabler

import com.ibm.gpuenabler.CUDARDDImplicits._
import com.ibm.gpuenabler.CUDADSImplicits._

import jcuda.jcublas.JCublas
import jcuda.{Pointer, Sizeof}
import org.apache.spark.SparkEnv
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf

class malMul {
  def run(args: Array[AnyRef]): Unit = {

    println("No of args " + args.length)
    val n = args(0) match {
      case ptr if ptr.isInstanceOf[Int] => ptr.asInstanceOf[Int]
      case _ => 0
    }
    val d_A1 = args(1) match {
      case ptr if ptr.isInstanceOf[Pointer] => ptr.asInstanceOf[Pointer]
      case _ => null
    }
    val d_B1 = args(2) match {
      case ptr if ptr.isInstanceOf[Pointer] => ptr.asInstanceOf[Pointer]
      case _ => null
    }
    val d_C1 = args(3) match {
      case ptr if ptr.isInstanceOf[Pointer] => ptr.asInstanceOf[Pointer]
      case _ => null
    }

    val alpha = 0.3f
    val beta = 0.7f
    val nn = Math.sqrt(n.toDouble).toInt
//    JCublas.cublasInit()

    JCublas.cublasSgemm(
      'n', 'n', nn, nn, nn, alpha, d_A1, nn, d_B1, nn, beta, d_C1, nn)

  }
}

case class Points(x: Float, y: Float)

object perfDebugMod {

  def sgemm(nn: Int, A: Array[Float], B: Array[Float], C: Array[Float]) = {

    val n = Math.sqrt(nn.toDouble).toInt
    val alpha = 0.3f
    val beta = 0.7f
    (0 until n ).foreach (i => {
      (0 until n ).foreach(j => {
        var prod: Float = 0
        (0 until n ). foreach(k => {
          prod += A(k * n + i) * B(j*n +k)
        })
        C(j * n + i) = alpha * prod + beta * C(j * n + i)
      })
    })
    C
  }

  def timeit(msg: String, code: => Any): Any ={
    val now1 = System.nanoTime
    code
    val ms1 = (System.nanoTime - now1) / 1000000
    println("%s Elapsed time: %d ms".format(msg, ms1))
  }

  def main(args : Array[String]): Unit = {

    val masterURL = if (args.length > 0) args(0) else "local[*]"
    val n: Long = if (args.length > 1) args(1).toLong else 9L
    // val n: Long = if (args.length > 1) args(1).toLong else 1000000L
    // val part = if (args.length > 2) args(2).toInt else 16
    val part = if (args.length > 2) args(2).toInt else 1

    val conf = new SparkConf(false).set("spark.executor.memory", "20g")
    val spark = SparkSession.builder().master(masterURL).appName("test").config(conf).getOrCreate()
    import spark.implicits._

    val sc = spark.sparkContext
    val ptxURL1 = "com.ibm.gpuenabler.malMul"

    val dsmapFunction = DSCUDAFunction(
      "run",
      Array("x", "y"),
      Array("value"),
      ptxURL1)

    val rd = spark.range(1, n+1, 1, part).map(x=> Points(x.toFloat,x.toFloat)).cache()
    rd.count()

    val data = rd.cacheGpu(true)
    // Load the data to GPU
    // data.loadGpu()

    timeit("cublasSgemm", {
      val mapDS = data.mapExtFunc(_.x, dsmapFunction).cacheGpu()
      mapDS.collect().foreach(println)
    })

   println("Expected: ")
   println()
    val nn: Int = n.toInt
    val A = Array.tabulate[Float](nn)(i => i.toFloat + 1.0f)
    val B = Array.tabulate[Float](nn)(i => i.toFloat + 1.0f)
    val C = Array.fill[Float](nn)(0)
    sgemm(nn, A, B, C).foreach(println)

  }
}


