package com.learning.ds.encoder

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.dsl.expressions.DslSymbol
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, Expression}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Encoder, Encoders}

import scala.reflect.ClassTag

class PersonEncoder {

  private var personEncoder: Encoder[Person] = _

  def createEncoder: Unit = {
    personEncoder = Encoders.product[Person]
  }

  def getSchemaAttachedToEncoder: StructType = {
    personEncoder.schema
  }

  def getEncoderType: ClassTag[Person] = {
    personEncoder.clsTag
  }

  def getSerExpressions: Seq[Expression] = {
    personEncoder.asInstanceOf[ExpressionEncoder[Person]].serializer
  }

  def getDeserExpression: Expression = {
    personEncoder.asInstanceOf[ExpressionEncoder[Person]].deserializer
  }

  def encodeAndDecode = {
    val person = Person(1, "Pardeep")

    val personExprEncoder: ExpressionEncoder[Person] = personEncoder.asInstanceOf[ExpressionEncoder[Person]]
    val row: InternalRow = personExprEncoder.toRow(person)

    val attrs: Seq[AttributeReference] = Seq(DslSymbol('id).long, DslSymbol('name).string)
    val personReborn: Person = personExprEncoder.resolveAndBind(attrs).fromRow(row)

    person == personReborn // true
  }

}

case class Person(id: Long, name: String)