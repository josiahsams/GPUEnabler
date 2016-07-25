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

case class MapExtFuncPhy(
                          func: Any => Any,
                          kernel: ExternalFunction,
                          outputObjAttr: Attribute,
                          tClassTag: Class[_],
                          uClassTag: Class[_],
                          child: SparkPlan) extends phy_UnaryNode with phy_ObjectOperator {

  override def output: Seq[Attribute] = outputObjAttr :: Nil

  override protected def doExecute(): RDD[InternalRow] = {
    val internalRowToRDD = child.execute().mapPartitions{ obj =>
      val getObject = unwrapObjectFromRow(child.output.head.dataType)

      obj.map(getObject)
    }

    internalRowToRDD.mapDSExtFunc(func, kernel, tClassTag, uClassTag).map{obj =>
      val outputObject = wrapObjectToRow(outputObjAttr.dataType)
      outputObject(obj)
    }
  }
}


case class MapExtFuncLog(func: Any => Any,
                         kernel: ExternalFunction,
                         outputObjAttr: Attribute,
                         tClassTag: Class[_],
                         uClassTag: Class[_],
                         child: LogicalPlan) extends UnaryNode 
                            with log_ObjectProducer

object MapExtFunc {
  def apply[T : Encoder, U : Encoder](
                        func: T => U,
                        kernel: ExternalFunction,
                        child: LogicalPlan): MapExtFuncLog = {
    MapExtFuncLog(
      func.asInstanceOf[Any => Any],
      kernel,
      CatalystSerde.generateObjAttr[U],
      _encoderFor[T].clsTag.runtimeClass,
      _encoderFor[U].clsTag.runtimeClass,
      child)
  }
}

object CUDADSImplicits {
  implicit class NewClass[T: Encoder](ds: Dataset[T]) {
    val sqlContext = ds.sqlContext
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
      case MapExtFuncLog(f, ker, outAttr, tc, uc, child) =>
        MapExtFuncPhy(f, ker, outAttr, tc, uc, planLater(child)) :: Nil
      case _ => {
        Nil
      }
    }
  }
  override def strategies: Seq[GenericStrategy[SparkPlan]] =
    (GPUOperators :: Nil) ++ getplanner(sqlContext).strategies
}

