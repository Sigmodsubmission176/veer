package edu.uci.ics.texera.workflow.common.operators

import com.fasterxml.jackson.annotation.JsonSubTypes.Type
import com.fasterxml.jackson.annotation.{JsonIgnore, JsonProperty, JsonSubTypes, JsonTypeInfo}
import com.microsoft.z3.{BoolExpr, Context}
import edu.uci.ics.amber.engine.common.virtualidentity.OperatorIdentity
import edu.uci.ics.amber.engine.operators.OpExecConfig
import edu.uci.ics.texera.workflow.common.metadata.{OperatorInfo, PropertyNameConstants}
import edu.uci.ics.texera.workflow.common.tuple.schema.{OperatorSchemaInfo, Schema}
import edu.uci.ics.texera.workflow.common.workflow.equitas.{SymbolicColumn, z3Utility}
import edu.uci.ics.texera.workflow.common.{ConstraintViolation, WorkflowContext}
import edu.uci.ics.texera.workflow.operators.aggregate.SpecializedAverageOpDesc
import edu.uci.ics.texera.workflow.operators.dictionary.DictionaryMatcherOpDesc
import edu.uci.ics.texera.workflow.operators.difference.DifferenceOpDesc
import edu.uci.ics.texera.workflow.operators.distinct.DistinctOpDesc
import edu.uci.ics.texera.workflow.operators.filter.SpecializedFilterOpDesc
import edu.uci.ics.texera.workflow.operators.hashJoin.HashJoinOpDesc
import edu.uci.ics.texera.workflow.operators.intersect.IntersectOpDesc
import edu.uci.ics.texera.workflow.operators.intervalJoin.IntervalJoinOpDesc
import edu.uci.ics.texera.workflow.operators.keywordSearch.KeywordSearchOpDesc
import edu.uci.ics.texera.workflow.operators.limit.LimitOpDesc
import edu.uci.ics.texera.workflow.operators.linearregression.LinearRegressionOpDesc
import edu.uci.ics.texera.workflow.operators.projection.ProjectionOpDesc
import edu.uci.ics.texera.workflow.operators.randomksampling.RandomKSamplingOpDesc
import edu.uci.ics.texera.workflow.operators.regex.RegexOpDesc
import edu.uci.ics.texera.workflow.operators.reservoirsampling.ReservoirSamplingOpDesc
import edu.uci.ics.texera.workflow.operators.sentiment.SentimentAnalysisOpDesc
import edu.uci.ics.texera.workflow.operators.source.apis.twitter.v2.TwitterFullArchiveSearchSourceOpDesc
import edu.uci.ics.texera.workflow.operators.source.scan.csv.CSVScanSourceOpDesc
import edu.uci.ics.texera.workflow.operators.source.scan.json.JSONLScanSourceOpDesc
import edu.uci.ics.texera.workflow.operators.source.sql.asterixdb.AsterixDBSourceOpDesc
import edu.uci.ics.texera.workflow.operators.source.sql.mysql.MySQLSourceOpDesc
import edu.uci.ics.texera.workflow.operators.source.sql.postgresql.PostgreSQLSourceOpDesc
import edu.uci.ics.texera.workflow.operators.unneststring.UnnestStringOpDesc
import edu.uci.ics.texera.workflow.operators.symmetricDifference.SymmetricDifferenceOpDesc
import edu.uci.ics.texera.workflow.operators.typecasting.TypeCastingOpDesc
import edu.uci.ics.texera.workflow.operators.udf.pythonV2.PythonUDFOpDescV2
import edu.uci.ics.texera.workflow.operators.udf.pythonV1.PythonUDFOpDesc
import edu.uci.ics.texera.workflow.operators.udf.pythonV2.source.PythonUDFSourceOpDescV2
import edu.uci.ics.texera.workflow.operators.union.UnionOpDesc
import edu.uci.ics.texera.workflow.operators.visualization.barChart.BarChartOpDesc
import edu.uci.ics.texera.workflow.operators.visualization.htmlviz.HtmlVizOpDesc
import edu.uci.ics.texera.workflow.operators.visualization.lineChart.LineChartOpDesc
import edu.uci.ics.texera.workflow.operators.visualization.pieChart.PieChartOpDesc
import edu.uci.ics.texera.workflow.operators.visualization.scatterplot.ScatterplotOpDesc
import edu.uci.ics.texera.workflow.operators.visualization.wordCloud.WordCloudOpDesc
import org.apache.commons.lang3.builder.{EqualsBuilder, EqualsExclude, HashCodeBuilder, ToStringBuilder}

import java.util.UUID
import edu.uci.ics.texera.workflow.operators.sink.managed.ProgressiveSinkOpDesc

import java.util
import scala.collection.mutable

@JsonTypeInfo(
  use = JsonTypeInfo.Id.NAME,
  include = JsonTypeInfo.As.PROPERTY,
  property = "operatorType"
)
@JsonSubTypes(
  Array(
    new Type(value = classOf[CSVScanSourceOpDesc], name = "CSVFileScan"),
    // disabled the ParallelCSVScanSourceOpDesc so that it does not confuse user. it can be re-enabled when doing experiments.
    // new Type(value = classOf[ParallelCSVScanSourceOpDesc], name = "ParallelCSVFileScan"),
    new Type(value = classOf[JSONLScanSourceOpDesc], name = "JSONLFileScan"),
    new Type(
      value = classOf[TwitterFullArchiveSearchSourceOpDesc],
      name = "TwitterFullArchiveSearch"
    ),
    new Type(value = classOf[ProgressiveSinkOpDesc], name = "SimpleSink"),
    new Type(value = classOf[RegexOpDesc], name = "Regex"),
    new Type(value = classOf[SpecializedFilterOpDesc], name = "Filter"),
    new Type(value = classOf[SentimentAnalysisOpDesc], name = "SentimentAnalysis"),
    new Type(value = classOf[ProjectionOpDesc], name = "Projection"),
    new Type(value = classOf[UnionOpDesc], name = "Union"),
    new Type(value = classOf[KeywordSearchOpDesc], name = "KeywordSearch"),
    new Type(value = classOf[SpecializedAverageOpDesc], name = "Aggregate"),
    new Type(value = classOf[LinearRegressionOpDesc], name = "LinearRegression"),
    new Type(value = classOf[LineChartOpDesc], name = "LineChart"),
    new Type(value = classOf[BarChartOpDesc], name = "BarChart"),
    new Type(value = classOf[PieChartOpDesc], name = "PieChart"),
    new Type(value = classOf[WordCloudOpDesc], name = "WordCloud"),
    new Type(value = classOf[HtmlVizOpDesc], name = "HTMLVisualizer"),
    new Type(value = classOf[ScatterplotOpDesc], name = "Scatterplot"),
    new Type(value = classOf[PythonUDFOpDesc], name = "PythonUDF"),
    new Type(value = classOf[PythonUDFOpDescV2], name = "PythonUDFV2"),
    new Type(value = classOf[PythonUDFSourceOpDescV2], name = "PythonUDFSourceV2"),
    new Type(value = classOf[MySQLSourceOpDesc], name = "MySQLSource"),
    new Type(value = classOf[PostgreSQLSourceOpDesc], name = "PostgreSQLSource"),
    new Type(value = classOf[AsterixDBSourceOpDesc], name = "AsterixDBSource"),
    new Type(value = classOf[TypeCastingOpDesc], name = "TypeCasting"),
    new Type(value = classOf[LimitOpDesc], name = "Limit"),
    new Type(value = classOf[RandomKSamplingOpDesc], name = "RandomKSampling"),
    new Type(value = classOf[ReservoirSamplingOpDesc], name = "ReservoirSampling"),
    new Type(value = classOf[HashJoinOpDesc[String]], name = "HashJoin"),
    new Type(value = classOf[DistinctOpDesc], name = "Distinct"),
    new Type(value = classOf[IntersectOpDesc], name = "Intersect"),
    new Type(value = classOf[SymmetricDifferenceOpDesc], name = "SymmetricDifference"),
    new Type(value = classOf[DifferenceOpDesc], name = "Difference"),
    new Type(value = classOf[IntervalJoinOpDesc], name = "IntervalJoin"),
    new Type(value = classOf[UnnestStringOpDesc], name = "UnnestString"),
    new Type(value = classOf[DictionaryMatcherOpDesc], name = "DictionaryMatcher")
  )
)
abstract class OperatorDescriptor extends Serializable {

  @EqualsExclude
  @JsonIgnore
  var context: WorkflowContext = _

  @JsonProperty(PropertyNameConstants.OPERATOR_ID)
  var operatorID: String = UUID.randomUUID.toString

  @EqualsExclude
  @JsonIgnore
  protected var symbolicColumns: mutable.Map[String, SymbolicColumn] = mutable.LinkedHashMap() //TODO populate when propagating schema
  @EqualsExclude
  @JsonIgnore
  protected var symbolicCondition: SymbolicColumn = null
  @EqualsExclude
  @JsonIgnore
  protected var variableConstraints: BoolExpr = null

  def operatorIdentifier: OperatorIdentity = OperatorIdentity(context.jobId, operatorID)

  def operatorExecutor(operatorSchemaInfo: OperatorSchemaInfo): OpExecConfig

  def operatorInfo: OperatorInfo

  def getOutputSchema(schemas: Array[Schema]): Schema

  def validate(): Array[ConstraintViolation] = {
    Array()
  }

  override def hashCode: Int = HashCodeBuilder.reflectionHashCode(this)

  override def equals(that: Any): Boolean = EqualsBuilder.reflectionEquals(this, that)

  override def toString: String = ToStringBuilder.reflectionToString(this)

  def setContext(workflowContext: WorkflowContext): Unit = {
    this.context = workflowContext
  }


  @JsonIgnore
  def getSymbolicCondition(): BoolExpr = this.symbolicCondition.isValueTrue
  @JsonIgnore
  def getVariableConstrainst(): BoolExpr = this.variableConstraints
  @JsonIgnore
  def setVariableConstrainst(constraint: BoolExpr): Unit = this.variableConstraints = constraint
  @JsonIgnore
  def setSymbolicCondition(tupleConditions: SymbolicColumn): Unit = this.symbolicCondition = tupleConditions

  def isEq(operator: OperatorDescriptor, z3Context: Context): Boolean = {
    if(checkSymbolicCondition(operator, z3Context)) {
      return checkSymbolicOutput(operator, z3Context)
    }
    false
  }

  /**
   * Check if the symbolic condition for this node is logically equivalent to that of another node.
   */
  def checkSymbolicCondition(operator: OperatorDescriptor, z3Context: Context): Boolean = {
    val condition1: SymbolicColumn = this.symbolicCondition
    val condition2: SymbolicColumn = operator.symbolicCondition
    if (z3Utility.isConditionEq(condition1.isValueTrue, condition2.isValueTrue, z3Context)) {
      this.joinCondition(condition1.isValueTrue, z3Context)
      operator.joinCondition(condition2.isValueTrue, z3Context)
      return true
    }
    false
  }

  // check if two nodes symbolic outputs are equivalent with default match
  def checkSymbolicOutput(operator: OperatorDescriptor, z3Context: Context): Boolean = {
    val symbolicOutputs1 = this.getSymbolicColumns
    val symbolicOutputs2 = operator.getSymbolicColumns
    z3Utility.symbolicOutputEqual(buildOutputEnv(operator, z3Context), symbolicOutputs1.values.toArray, symbolicOutputs2.values.toArray, z3Context)
  }

  private def buildOutputEnv(operator: OperatorDescriptor, z3Context: Context): BoolExpr = {
    val env = new util.ArrayList[BoolExpr]
    env.add(this.getVariableConstraints)
    env.add(operator.getVariableConstraints)
    z3Utility.mkAnd(env, z3Context)
  }

  @JsonIgnore
  // return the symbolic output tuple
  def getSymbolicColumns: mutable.Map[String, SymbolicColumn] = this.symbolicColumns

  @JsonIgnore
  def getVariableConstraints: BoolExpr = this.variableConstraints

  def joinCondition(condition: BoolExpr, z3Context: Context): Unit = {
    this.variableConstraints = z3Context.mkAnd(this.variableConstraints, condition)
  }

  def constructSR(upstreamOperators: Array[OperatorDescriptor], z3Context: Context): Unit = {
    this.constructSymbolicColumns(upstreamOperators, z3Context)
    this.constructSymbolicCondition(upstreamOperators, z3Context)
    this.constructVariableConstraints(upstreamOperators, z3Context)
  }

  @JsonIgnore
  def isSymbolicNotConstructed(): Boolean = {
    symbolicColumns.isEmpty && symbolicCondition == null
  }

  def constructVariableConstraints(upstreamOperators: Array[OperatorDescriptor], z3Context: Context): Unit = {
    this.variableConstraints = upstreamOperators.head.variableConstraints
  }
  def constructSymbolicCondition(upstreamOperators: Array[OperatorDescriptor], z3Context: Context): Unit = {
this.symbolicCondition = upstreamOperators.head.symbolicCondition
    //    upstreamOperators.foreach(upstreamOp =>
//    upstreamOperators.symbolicColumns.foreach(column => this.symbolicColumns += column)
//    )
  }

  def constructSymbolicColumns(upstreamOperators: Array[OperatorDescriptor], z3Context: Context): Unit = {
    this.symbolicColumns = upstreamOperators.head.symbolicColumns
  }

  def clearSymbolicRepresentation(): Unit = {
    this.symbolicCondition = null
    this.symbolicColumns.clear()
    this.variableConstraints = null
  }
}
