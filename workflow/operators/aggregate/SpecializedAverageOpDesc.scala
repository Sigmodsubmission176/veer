package edu.uci.ics.texera.workflow.operators.aggregate

import com.fasterxml.jackson.annotation.{JsonIgnore, JsonProperty, JsonPropertyDescription}
import com.kjetland.jackson.jsonSchema.annotations.JsonSchemaTitle
import com.microsoft.z3.Context
import edu.uci.ics.texera.workflow.common.metadata.annotations.{AutofillAttributeName, AutofillAttributeNameList}
import edu.uci.ics.texera.workflow.common.metadata.{InputPort, OperatorGroupConstants, OperatorInfo, OutputPort}
import edu.uci.ics.texera.workflow.common.operators.OperatorDescriptor
import edu.uci.ics.texera.workflow.common.operators.aggregate.{AggregateOpDesc, AggregateOpExecConfig, DistributedAggregation}
import edu.uci.ics.texera.workflow.common.tuple.Tuple
import edu.uci.ics.texera.workflow.common.tuple.schema.AttributeTypeUtils.parseTimestamp
import edu.uci.ics.texera.workflow.common.tuple.schema.{AttributeType, OperatorSchemaInfo, Schema}
import edu.uci.ics.texera.workflow.common.workflow.drove.Equitas
import edu.uci.ics.texera.workflow.common.workflow.equitas.SymbolicColumn

import java.io.Serializable
import scala.collection.mutable

case class AveragePartialObj(sum: Double, count: Double) extends Serializable {}

class SpecializedAverageOpDesc extends AggregateOpDesc {

  @JsonProperty(required = true)
  @JsonSchemaTitle("Aggregation Function")
  @JsonPropertyDescription("sum, count, average, min, or max")
  var aggFunction: AggregationFunction = _

  @JsonProperty(value = "attribute", required = true)
  @JsonPropertyDescription("column to calculate average value")
  @AutofillAttributeName
  var attribute: String = _

  @JsonProperty(value = "result attribute", required = true)
  @JsonPropertyDescription("column name of average result")
  var resultAttribute: String = _

  @JsonProperty("groupByKeys")
  @JsonSchemaTitle("Group By Keys")
  @JsonPropertyDescription("group by columns")
  @AutofillAttributeNameList
  var groupByKeys: List[String] = _

  @JsonIgnore
  private var groupBySchema: Schema = _
  @JsonIgnore
  private var finalAggValueSchema: Schema = _

  override def operatorExecutor(
                                 operatorSchemaInfo: OperatorSchemaInfo
                               ): AggregateOpExecConfig[_] = {
    this.groupBySchema = getGroupByKeysSchema(operatorSchemaInfo.inputSchemas)
    this.finalAggValueSchema = getFinalAggValueSchema

    aggFunction match {
      case AggregationFunction.AVERAGE => averageAgg(operatorSchemaInfo)
      case AggregationFunction.COUNT => countAgg(operatorSchemaInfo)
      case AggregationFunction.MAX => maxAgg(operatorSchemaInfo)
      case AggregationFunction.MIN => minAgg(operatorSchemaInfo)
      case AggregationFunction.SUM => sumAgg(operatorSchemaInfo)
      case _ =>
        throw new UnsupportedOperationException("Unknown aggregation function: " + aggFunction)
    }
  }

  def sumAgg(operatorSchemaInfo: OperatorSchemaInfo): AggregateOpExecConfig[_] = {
    val aggregation = new DistributedAggregation[java.lang.Double](
      () => 0,
      (partial, tuple) => {
        val value = getNumericalValue(tuple)
        partial + (if (value.isDefined) value.get else 0)

      },
      (partial1, partial2) => partial1 + partial2,
      partial => {
        Tuple
          .newBuilder(finalAggValueSchema)
          .add(resultAttribute, AttributeType.DOUBLE, partial)
          .build
      },
      groupByFunc()
    )
    new AggregateOpExecConfig[java.lang.Double](
      operatorIdentifier,
      aggregation,
      operatorSchemaInfo
    )
  }

  def countAgg(operatorSchemaInfo: OperatorSchemaInfo): AggregateOpExecConfig[_] = {
    val aggregation = new DistributedAggregation[Integer](
      () => 0,
      (partial, tuple) => {
        partial + (if (tuple.getField(attribute) != null) 1 else 0)
      },
      (partial1, partial2) => partial1 + partial2,
      partial => {
        Tuple
          .newBuilder(finalAggValueSchema)
          .add(resultAttribute, AttributeType.INTEGER, partial)
          .build
      },
      groupByFunc()
    )
    new AggregateOpExecConfig[Integer](
      operatorIdentifier,
      aggregation,
      operatorSchemaInfo
    )
  }

  def minAgg(operatorSchemaInfo: OperatorSchemaInfo): AggregateOpExecConfig[_] = {
    val aggregation = new DistributedAggregation[java.lang.Double](
      () => Double.MaxValue,
      (partial, tuple) => {
        val value = getNumericalValue(tuple)
        if (value.isDefined && value.get < partial) value.get else partial
      },
      (partial1, partial2) => if (partial1 < partial2) partial1 else partial2,
      partial => {
        if (partial == Double.MaxValue) null
        else
          Tuple
            .newBuilder(finalAggValueSchema)
            .add(resultAttribute, AttributeType.DOUBLE, partial)
            .build
      },
      groupByFunc()
    )
    new AggregateOpExecConfig[java.lang.Double](
      operatorIdentifier,
      aggregation,
      operatorSchemaInfo
    )
  }

  def maxAgg(operatorSchemaInfo: OperatorSchemaInfo): AggregateOpExecConfig[_] = {
    val aggregation = new DistributedAggregation[java.lang.Double](
      () => Double.MinValue,
      (partial, tuple) => {
        val value = getNumericalValue(tuple)
        if (value.isDefined && value.get > partial) value.get else partial
      },
      (partial1, partial2) => if (partial1 > partial2) partial1 else partial2,
      partial => {
        if (partial == Double.MinValue) null
        else
          Tuple
            .newBuilder(finalAggValueSchema)
            .add(resultAttribute, AttributeType.DOUBLE, partial)
            .build
      },
      groupByFunc()
    )
    new AggregateOpExecConfig[java.lang.Double](
      operatorIdentifier,
      aggregation,
      operatorSchemaInfo
    )
  }

  def groupByFunc(): Schema => Schema = {
    if (this.groupByKeys == null) null
    else
      schema => {
        // Since this is a partially evaluated tuple, there is no actual schema for this
        // available anywhere. Constructing it once for re-use
        if (groupBySchema == null) {
          val schemaBuilder = Schema.newBuilder()
          groupByKeys.foreach(key => schemaBuilder.add(schema.getAttribute(key)))
          groupBySchema = schemaBuilder.build
        }
        groupBySchema
      }
  }

  private def getNumericalValue(tuple: Tuple): Option[Double] = {
    val value: Object = tuple.getField(attribute)
    if (value == null)
      return None

    if (tuple.getSchema.getAttribute(attribute).getType == AttributeType.TIMESTAMP)
      Option(parseTimestamp(value.toString).getTime.toDouble)
    else Option(value.toString.toDouble)
  }

  def averageAgg(operatorSchemaInfo: OperatorSchemaInfo): AggregateOpExecConfig[_] = {
    val aggregation = new DistributedAggregation[AveragePartialObj](
      () => AveragePartialObj(0, 0),
      (partial, tuple) => {
        val value = getNumericalValue(tuple)
        AveragePartialObj(
          partial.sum + (if (value.isDefined) value.get else 0),
          partial.count + (if (value.isDefined) 1 else 0)
        )
      },
      (partial1, partial2) =>
        AveragePartialObj(partial1.sum + partial2.sum, partial1.count + partial2.count),
      partial => {
        val value = if (partial.count == 0) null else partial.sum / partial.count
        Tuple
          .newBuilder(finalAggValueSchema)
          .add(resultAttribute, AttributeType.DOUBLE, value)
          .build
      },
      groupByFunc()
    )
    new AggregateOpExecConfig[AveragePartialObj](
      operatorIdentifier,
      aggregation,
      operatorSchemaInfo
    )
  }

  private def getGroupByKeysSchema(schemas: Array[Schema]): Schema = {
    if (groupByKeys == null) {
      groupByKeys = List()
    }
    Schema
      .newBuilder()
      .add(groupByKeys.map(key => schemas(0).getAttribute(key)).toArray: _*)
      .build()
  }

  private def getFinalAggValueSchema: Schema = {
    if (this.aggFunction.equals(AggregationFunction.COUNT)) {
      Schema
        .newBuilder()
        .add(resultAttribute, AttributeType.INTEGER)
        .build()
    } else {
      Schema
        .newBuilder()
        .add(resultAttribute, AttributeType.DOUBLE)
        .build()
    }
  }

  override def operatorInfo: OperatorInfo =
    OperatorInfo(
      "Aggregate",
      "Calculate different types of aggregation values",
      OperatorGroupConstants.UTILITY_GROUP,
      inputPorts = List(InputPort()),
      outputPorts = List(OutputPort())
    )

  override def getOutputSchema(schemas: Array[Schema]): Schema = {
    if (resultAttribute == null || resultAttribute.trim.isEmpty) {
      return null
    }
    Schema
      .newBuilder()
      .add(getGroupByKeysSchema(schemas).getAttributes)
      .add(getFinalAggValueSchema.getAttributes)
      .build()
  }

  override def equals(that: Any): Boolean = {
    if (that != null) {
      if (classOf[SpecializedAverageOpDesc].isInstance(that)) {
        val other = that.asInstanceOf[SpecializedAverageOpDesc]
        return this.aggFunction.getName.equals(other.aggFunction.getName) && this.attribute.equals(other.attribute) && this.groupByKeys.equals(other.groupByKeys)
      }
    }
    return false
  }
  override def constructSymbolicColumns(upstreamOperators: Array[OperatorDescriptor], z3Context: Context): Unit = {
    this.symbolicColumns = mutable.LinkedHashMap()
    this.symbolicColumns ++= this.getSymbolicGroupByColumns(upstreamOperators(0))
    this.symbolicColumns.put(this.resultAttribute, this.constructSymbolicAggCalls(upstreamOperators(0), z3Context))
  }

  private def getSymbolicGroupByColumns(upstreamOperator: OperatorDescriptor): mutable.Map[String, SymbolicColumn] = {
    var symbolicGroupByColumns: mutable.Map[String, SymbolicColumn] = mutable.LinkedHashMap()
    for (groupingColumn <- groupByKeys) {
      symbolicGroupByColumns.put(groupingColumn, upstreamOperator.getSymbolicColumns.get(groupingColumn).get)
    }
    symbolicGroupByColumns
  }

  private def constructSymbolicAggCalls(upstreamOperator: OperatorDescriptor, z3Context: Context): SymbolicColumn = {
    // check the global variable if it has the function
    var SYMBOLIC_AGGREGATE = Equitas.SYMBOLIC_AGGREGATE
    // yes retrieve that SR
    if (SYMBOLIC_AGGREGATE.containsKey(this.aggFunction.getName+"("+this.attribute+")")) {
      return SYMBOLIC_AGGREGATE.get(this.aggFunction.getName+"("+this.attribute+")")
    }
    // not construct new and add it to the list
    else {
      val newColumn = SymbolicColumn.mkNewSymbolicColumn(z3Context, upstreamOperator.getSymbolicColumns.get(attribute).get.getType)
      SYMBOLIC_AGGREGATE.put(this.aggFunction.getName+"("+this.attribute+")", newColumn)
      return newColumn
    }
  }

}
