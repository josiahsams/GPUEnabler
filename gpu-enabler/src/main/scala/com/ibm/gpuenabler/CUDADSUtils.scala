package com.ibm.gpuenabler

import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.UnresolvedDeserializer
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression, Literal, NamedExpression, PrettyAttribute}
import org.apache.spark.sql.catalyst.planning.{GenericStrategy, QueryPlanner}
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, UnaryNode}
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.gpuenabler.Utils._
import com.ibm.gpuenabler.CUDARDDImplicits._
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.catalyst.plans.logical.CatalystSerde

import scala.reflect.ClassTag

/**
  * Created by joe on 15/7/16.
  */
/*
case class MapExtFuncPhy[T, U](
                           func: Any => Any,
                           kernel: ExternalFunction,
                           tEncoder: ExpressionEncoder[T],
                           uEncoder: ExpressionEncoder[U],
                           child: SparkPlan) extends phy_UnaryNode with phy_ObjectOperator {
val deserializer = UnresolvedDeserializer(tEncoder.deserializer)
val serializer = uEncoder.namedExpressions
  */

case class MapExtFuncPhy(
                          func: Any => Any,
                          kernel: ExternalFunction,
                          deserializer: Expression,
                          serializer: Seq[NamedExpression],
                          tClassTag: Class[_],
                          uClassTag: Class[_],
                          child: SparkPlan) extends phy_UnaryNode with phy_ObjectOperator {


  override def output: Seq[Attribute] = serializer.map(_.toAttribute)

  override protected def doExecute(): RDD[InternalRow] = {
    //child.execute().mapPartitionsInternal { iter =>

    /*
    child.execute().mapPartitions { iter =>
      val getObject = generateToObject(deserializer,child.output )
      val outputObject = generateToRow(serializer)
      //func(iter.map(getObject))
      iter.map(getObject).map(func).map(outputObject)
    }
     */

    /*
    child.execute().map(row => {
      val getObject = generateToObject(deserializer,child.output )
      val outputObject = generateToRow(serializer)
      outputObject(func(getObject(row)))
    })*/

    val internalRowToRDD = child.execute().map{ obj =>
      // val getObject = generateToObject(deserializer,child.output )
      val getObject = deserializeRowToObject(deserializer,child.output )
      getObject(obj)
    }

    internalRowToRDD.mapDSExtFunc(func, kernel, tClassTag, uClassTag).map{obj =>
      val outputObject = serializeObjectToRow(serializer)
      // val outputObject = generateToRow(serializer)
      outputObject(obj)
    }
  }
}


case class MapExtFuncLog(func: Any => Any,
                         kernel: ExternalFunction,
                         outputObjAttr: Attribute,
                         deserializer: Expression,
                         serializer: Seq[NamedExpression],
                         tClassTag: Class[_],
                         uClassTag: Class[_],
                         child: LogicalPlan) extends UnaryNode 
                            with log_ObjectProducer
                           // with log_ObjectConsumer with log_ObjectProducer
                         // child: LogicalPlan) extends UnaryNode with log_ObjectOperator
/*
case class MapExtFuncLog[T , U ](func: Any => Any,
                      kernel: ExternalFunction,
                      tEncoder: ExpressionEncoder[T],
                      uEncoder: ExpressionEncoder[U],
                      child: LogicalPlan) extends UnaryNode with log_ObjectOperator {
override def serializer: Seq[NamedExpression] = uEncoder.namedExpressions
}
*/

object MapExtFunc {
  def apply[T : Encoder, U : Encoder](
                        func: T => U,
                        kernel: ExternalFunction,
                        child: LogicalPlan): MapExtFuncLog = {

    //val inputAttr = inputCols.map(call_named(_).toAttribute)
//    _encoderFor[T].clsTag.runtimeClass match {
  //    case c if c == classOf[Byte] => Nil
   // }

    MapExtFuncLog(
      func.asInstanceOf[Any => Any],
      kernel,
      CatalystSerde.generateObjAttr[U],
      UnresolvedDeserializer(_encoderFor[T].deserializer),
      _encoderFor[U].namedExpressions,
      _encoderFor[T].clsTag.runtimeClass,
      _encoderFor[U].clsTag.runtimeClass,
      child)

    /*
    val tEncoder = _encoderFor[T]
    val uEncoder = _encoderFor[U]

    MapExtFuncLog(
      func.asInstanceOf[Any => Any],
      kernel,
      tEncoder, uEncoder,
      child)
      */
  }
}

object CUDADSImplicits {
  implicit class NewClass[T: Encoder](ds: Dataset[T]) {
    val sqlContext = ds.sqlContext
    // val logicalplan = ds.queryExecution.analyzed
    val logicalplan = getLogicalPlan(ds)

    def mapExtFunc[U: Encoder](func: T => U, kernel: ExternalFunction): Dataset[U] = new Dataset[U](
      sqlContext,
      MapExtFunc[T, U](func, kernel, logicalplan),
      implicitly[Encoder[U]])

    sqlContext.experimental.extraStrategies =
      (new MapExtFuncStrategy(sqlContext).GPUOperators :: Nil)
  }
}

class MapExtFuncStrategy(sqlContext: SQLContext) extends QueryPlanner[SparkPlan] {
  object GPUOperators extends Strategy {
    def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
      case MapExtFuncLog(f, ker, outAttr, in, out, tc, uc, child) =>
        MapExtFuncPhy(f, ker, in, out, tc, uc, planLater(child)) :: Nil
      case _ => {
        Nil
      }
    }
  }
  override def strategies: Seq[GenericStrategy[SparkPlan]] =
    (GPUOperators :: Nil) ++ getplanner(sqlContext).strategies
}

