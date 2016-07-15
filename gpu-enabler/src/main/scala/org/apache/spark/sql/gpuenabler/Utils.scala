package org.apache.spark.sql.gpuenabler

import org.apache.spark.sql.catalyst.encoders._
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.{Column, Dataset, Encoder, SQLContext}

/**
  * Created by joe on 15/7/16.
  */
object Utils {

  type _Dataset[T] = Dataset[T]

  def _Dataset[T: Encoder](sqc: _SQLContext, lp: LogicalPlan) = {
    Dataset[T](sqc, lp)
  }
  type _SQLContext = SQLContext

  def get_resolvedTEncoder[T: Encoder](ds: Dataset[T]): ExpressionEncoder[T] =
    ds.resolvedTEncoder

  def _encoderFor[A : Encoder]: ExpressionEncoder[A] = encoderFor[A]

  def getAttributes(schema : org.apache.spark.sql.types.StructType) = schema.toAttributes

  type phy_UnaryNode = org.apache.spark.sql.execution.UnaryNode

  type log_UnaryNode = org.apache.spark.sql.catalyst.plans.logical.UnaryNode

  type phy_ObjectOperator = org.apache.spark.sql.execution.ObjectOperator

  type log_ObjectOperator = org.apache.spark.sql.catalyst.plans.logical.ObjectOperator

  def getplanner(sqlContext: SQLContext) = sqlContext.sessionState.planner

  def getresolver(sqlContext: SQLContext) = sqlContext.sessionState.analyzer.resolver

  def call_named(col: Column) = col.named
}