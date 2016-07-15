package scala.com.ibm.gpuenabler

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.Column
import org.apache.spark.sql.catalyst.expressions.{Literal, PrettyAttribute}
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.{SparkConf, SparkContext}
import com.ibm.gpuenabler.CUDADSImplicits._

/**
  * Created by joe on 15/7/16.
  */
case class Person(name: String, age: Long, count: Long)

object GpuEnablerDS {

  def main(args: Array[String]) = {
    val sc = new SparkContext(new SparkConf().setAppName("DSTest").setMaster("local"))

    // val sparksession = SparkSession.builder().master("local").appName("test").getOrCreate()
    val logger = Logger.getRootLogger
    logger.setLevel(Level.ERROR)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._

    val ds = sqlContext.read.json("examples/src/main/resources/people.json").as[Person]
    ds.mapExtFunc(Seq(ds("age"),ds("count")), (x:Person) => x.age).show()

    ds.map2(Seq(ds("age"),ds("count")),Seq(PrettyAttribute("newCol1", DoubleType),
      PrettyAttribute("newCol2", DoubleType), PrettyAttribute("age", DoubleType))).show()

    println(" ======= ")
    ds.map((x:Person) => x.age).show()

    ds.select(ds("age"),ds("count")).withColumn("NEWCol", ds("age")+ds("count")).show()
    ds.select(ds("age"),ds("count")).withColumn("NEWCol", new Column(Literal(0))).show()


    ds.select(ds("age"),ds("count"),  new Column(Literal(5)).as("FIVE")).show()
  }
}
