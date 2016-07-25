package com.ibm.gpuenabler

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.Column
import org.apache.spark.sql.catalyst.expressions.{Literal}
import org.apache.spark.{SparkConf, SparkContext}
import com.ibm.gpuenabler.CUDADSImplicits._
import com.ibm.gpuenabler.GpuEnablerExample.{getClass => _, _}

/**
  * Created by joe on 15/7/16.
  */
case class Person(name: Long, age: Long, count: Long)

object GpuEnablerDS {

  def main(args: Array[String]) = {
    val sc = new SparkContext(new SparkConf().setAppName("DSTest").setMaster("local"))

    // val sparksession = SparkSession.builder().master("local").appName("test").getOrCreate()
    val logger = Logger.getRootLogger
    logger.setLevel(Level.ERROR)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._

    val ptxURL = this.getClass.getResource("/GpuEnablerExamples.ptx")
    val mapFunction = new CUDAFunction(
      "multiplylongBy2",
      Array("this.age"),
      Array("this"),
      ptxURL)


    val ds = sqlContext.read.json("src/main/resources/people.json").as[Person]
    ds.mapExtFunc((x:Person) => x.age, mapFunction ).show()

    println(" ======= ")
    ds.map((x:Person) => x.age).show()

    ds.select(ds("age"),ds("count")).withColumn("NEWCol", ds("age")+ds("count")).show()
    ds.select(ds("age"),ds("count")).withColumn("NEWCol", new Column(Literal(0))).show()


    ds.select(ds("age"),ds("count"),  new Column(Literal(5)).as("FIVE")).show()
  }
}
