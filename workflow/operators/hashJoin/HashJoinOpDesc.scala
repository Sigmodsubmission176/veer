package edu.uci.ics.texera.workflow.operators.hashJoin

import com.fasterxml.jackson.annotation.{JsonIgnore, JsonProperty, JsonPropertyDescription}
import com.google.common.base.Preconditions
import com.kjetland.jackson.jsonSchema.annotations.JsonSchemaTitle
import com.microsoft.z3.{BoolExpr, Context}
import edu.uci.ics.amber.engine.operators.OpExecConfig
import edu.uci.ics.texera.workflow.common.metadata.annotations.{AutofillAttributeName, AutofillAttributeNameOnPort1}
import edu.uci.ics.texera.workflow.common.metadata.{InputPort, OperatorGroupConstants, OperatorInfo, OutputPort}
import edu.uci.ics.texera.workflow.common.operators.OperatorDescriptor
import edu.uci.ics.texera.workflow.common.tuple.schema.{Attribute, OperatorSchemaInfo, Schema}
import edu.uci.ics.texera.workflow.common.workflow.equitas.{SymbolicColumn, z3Utility}
import edu.uci.ics.texera.workflow.operators.filter.{ComparisonType, FilterPredicate}

import java.util
import scala.collection.convert.ImplicitConversions.`collection AsScalaIterable`
import scala.collection.mutable
import scala.jdk.CollectionConverters.{mapAsJavaMap, mutableSeqAsJavaListConverter}

class HashJoinOpDesc[K] extends OperatorDescriptor {

  @JsonProperty(required = true)
  @JsonSchemaTitle("Left Input Attribute")
  @JsonPropertyDescription("attribute to be joined on the Left Input")
  @AutofillAttributeName
  var buildAttributeName: String = _

  @JsonProperty(required = true)
  @JsonSchemaTitle("Right Input Attribute")
  @JsonPropertyDescription("attribute to be joined on the Right Input")
  @AutofillAttributeNameOnPort1
  var probeAttributeName: String = _

  @JsonProperty(required = true, defaultValue = "inner")
  @JsonSchemaTitle("Join Type")
  @JsonPropertyDescription("select the join type to execute")
  var joinType: JoinType = JoinType.INNER

  @JsonIgnore
  var opExecConfig: HashJoinOpExecConfig[K] = _

  override def operatorExecutor(operatorSchemaInfo: OperatorSchemaInfo): OpExecConfig = {
    opExecConfig = new HashJoinOpExecConfig[K](
      operatorIdentifier,
      probeAttributeName,
      buildAttributeName,
      joinType,
      operatorSchemaInfo
    )
    opExecConfig
  }

  override def operatorInfo: OperatorInfo =
    OperatorInfo(
      "Hash Join",
      "join two inputs",
      OperatorGroupConstants.JOIN_GROUP,
      inputPorts = List(InputPort("left"), InputPort("right")),
      outputPorts = List(OutputPort())
    )

  // remove the probe attribute in the output
  override def getOutputSchema(schemas: Array[Schema]): Schema = {
    Preconditions.checkArgument(schemas.length == 2)
    val builder = Schema.newBuilder()
    val buildSchema = schemas(0)
    val probeSchema = schemas(1)
    builder.add(buildSchema).removeIfExists(probeAttributeName)
    if (probeAttributeName.equals(buildAttributeName)) {
      probeSchema.getAttributes.foreach(attr => {
        val attributeName = attr.getName
        if (buildSchema.containsAttribute(attributeName) && attributeName != probeAttributeName) {
          // appending 1 to the output of Join schema in case of duplicate attributes in probe and build table
          builder.add(new Attribute(s"$attributeName#@1", attr.getType))
        } else {
          builder.add(attr)
        }
      })

    } else {
      probeSchema.getAttributes
        .forEach(attr => {
          val attributeName = attr.getName
          if (buildSchema.containsAttribute(attributeName)) {
            builder.add(new Attribute(s"$attributeName#@1", attr.getType))
          } else if (!attributeName.equalsIgnoreCase(probeAttributeName)) {
            builder.add(attr)
          }
        })
    }
    builder.build()
  }

  override def equals(that: Any): Boolean = {
    if (that == null || (getClass != that.getClass)) {
       false
    }
    else {
      val other = that.asInstanceOf[HashJoinOpDesc[K]]
      this.probeAttributeName.equals(other.probeAttributeName) && this.buildAttributeName.equals(other.buildAttributeName) && this.joinType.getJoinType.equals(other.joinType.getJoinType)
    }
 }

  override def constructSymbolicCondition(upstreamOperators: Array[OperatorDescriptor], z3Context: Context): Unit = {
    var assign: mutable.MutableList[BoolExpr] = mutable.MutableList()
    assign += upstreamOperators(0).getVariableConstraints
    assign += upstreamOperators(1).getVariableConstraints
    this.setSymbolicCondition(z3Utility.joinSymbolicCoumns(upstreamOperators, z3Utility.getNodeSymbolicColumn(true, false, mapAsJavaMap(upstreamOperators(0).getSymbolicColumns ++ upstreamOperators(1).getSymbolicColumns), util.Arrays.asList(new FilterPredicate(buildAttributeName, ComparisonType.EQUAL_TO, probeAttributeName)), z3Context), z3Context))
    this.setVariableConstrainst(z3Utility.mkAnd(assign.asJava, z3Context))
  }

  override def constructSymbolicColumns(upstreamOperators: Array[OperatorDescriptor], z3Context: Context): Unit = {
    // TODO use the config after compiling to find the build table
    val buildSymbolicColumns: mutable.Map[String, SymbolicColumn] = if(upstreamOperators(0).getSymbolicColumns.size < upstreamOperators(1).getSymbolicColumns.size) upstreamOperators(0).getSymbolicColumns.clone() else upstreamOperators(1).getSymbolicColumns.clone()
    val probeSymbolicColumns: mutable.Map[String, SymbolicColumn] = if(upstreamOperators(0).getSymbolicColumns.size < upstreamOperators(1).getSymbolicColumns.size) upstreamOperators(1).getSymbolicColumns.clone() else upstreamOperators(0).getSymbolicColumns.clone()
    this.symbolicColumns = this.symbolicColumns ++ buildSymbolicColumns
    probeSymbolicColumns.retain((key, column) => !key.equalsIgnoreCase(probeAttributeName)).foreach(column =>
          if(this.symbolicColumns.contains(column._1)){
            this.symbolicColumns.put(column._1+"#@1", column._2)
        }
      else{
            this.symbolicColumns.put(column._1, column._2)
          }
    )
  }
}
