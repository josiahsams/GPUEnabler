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

/**
  * Created by joe on 15/7/16.
  */

case class MapExtFuncPhy(
                           func: Iterator[Any] => Iterator[Any],
                           inputAttr: Seq[Attribute],
                           deserializer: Expression,
                           serializer: Seq[NamedExpression],
                           child: SparkPlan) extends phy_UnaryNode with phy_ObjectOperator {
  override def output: Seq[Attribute] = serializer.map(_.toAttribute) ++ inputAttr

  override protected def doExecute(): RDD[InternalRow] = {
    //child.execute().mapPartitionsInternal { iter =>
    child.execute().mapPartitions { iter =>
      val getObject = generateToObject(deserializer,child.output )
      val outputObject = generateToRow(serializer)
      func(iter.map(getObject)).map(outputObject)
    }
  }
}

case class MapExtFuncLog(func: Iterator[Any] => Iterator[Any],
                        inputCols: Seq[Attribute],
                        deserializer: Expression,
                        serializer: Seq[NamedExpression],
                        child: LogicalPlan) extends UnaryNode with log_ObjectOperator

object MapExtFunc {
  def apply[T : Encoder, U : Encoder](
                                       func: Iterator[T] => Iterator[U],
                                       inputCols: Seq[Column],
                                       child: LogicalPlan): MapExtFuncLog = {
    val inputAttr = inputCols.map(call_named(_).toAttribute)

//    _encoderFor[T].clsTag.runtimeClass match {
  //    case c if c == classOf[Byte] => Nil
   // }

    MapExtFuncLog(
      func.asInstanceOf[Iterator[Any] => Iterator[Any]],
      inputAttr,
      UnresolvedDeserializer(_encoderFor[T].deserializer),
      _encoderFor[U].namedExpressions,
      child)
  }
}

object CUDADSImplicits {
  implicit class NewClass[T: Encoder](ds: Dataset[T]) {
    val sqlContext = ds.sqlContext
    val logicalplan = ds.queryExecution.analyzed

    def mapExtFunc[U: Encoder](inputCols: Seq[Column], func: T => U): _Dataset[U] = new Dataset[U](
      sqlContext,
      MapExtFunc[T, U](_.map(func), inputCols, logicalplan),
      implicitly[Encoder[U]])

    def getExpr(colName: PrettyAttribute): Column = {
      val resolver = getresolver(sqlContext)
      val output = ds.queryExecution.analyzed.output
      val shouldReplace = output.exists(f => resolver(f.name, colName.name))
      if (shouldReplace) {
        val column = output.map { field =>
          if (resolver(field.name, colName.name)) {
            new Column(field)
          } else {
            new Column(Literal.create(0.0, colName.dataType)).as(colName.name)
          }
        }
        column.head
      } else {
        new Column(Literal.create(0.0, colName.dataType)).as(colName.name)
      }
    }

    def map2(inputCols: Seq[Column], outputCols: Seq[PrettyAttribute]) = {
      val combinedCols = inputCols ++ outputCols.map(getExpr)
      ds.select(combinedCols: _*)
    }

    sqlContext.experimental.extraStrategies =
      (new MapExtFuncStrategy(sqlContext).GPUOperators :: Nil)
  }
}

class MapExtFuncStrategy(sqlContext: SQLContext) extends QueryPlanner[SparkPlan] {
  object GPUOperators extends Strategy {
    def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
      case MapExtFuncLog(f, incol, in, out, child) =>
        MapExtFuncPhy(f, incol, in, out, planLater(child)) :: Nil
      case _ => {
        Nil
      }
    }
  }
  override def strategies: Seq[GenericStrategy[SparkPlan]] =
    (GPUOperators :: Nil) ++ getplanner(sqlContext).strategies
}

