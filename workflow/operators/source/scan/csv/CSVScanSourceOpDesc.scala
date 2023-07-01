package edu.uci.ics.texera.workflow.operators.source.scan.csv

import com.fasterxml.jackson.annotation.{JsonIgnore, JsonProperty, JsonPropertyDescription}
import com.github.tototoshi.csv.{CSVReader, DefaultCSVFormat}
import com.kjetland.jackson.jsonSchema.annotations.JsonSchemaTitle
import edu.uci.ics.amber.engine.operators.OpExecConfig
import edu.uci.ics.texera.workflow.common.operators.{ManyToOneOpExecConfig, OperatorDescriptor}
import edu.uci.ics.texera.workflow.common.tuple.schema.AttributeTypeUtils.inferSchemaFromRows
import edu.uci.ics.texera.workflow.common.tuple.schema.{Attribute, AttributeType, OperatorSchemaInfo, Schema}
import edu.uci.ics.texera.workflow.operators.source.scan.ScanSourceOpDesc
import com.fasterxml.jackson.databind.annotation.JsonDeserialize
import com.microsoft.z3.Context
import edu.uci.ics.texera.workflow.common.workflow.equitas.SymbolicColumn
import org.apache.commons.lang3.builder.EqualsExclude

import java.io.IOException
import scala.jdk.CollectionConverters.asJavaIterableConverter

class CSVScanSourceOpDesc extends ScanSourceOpDesc {

  @JsonProperty(defaultValue = ",")
  @JsonSchemaTitle("Delimiter")
  @JsonPropertyDescription("delimiter to separate each line into fields")
  @JsonDeserialize(contentAs = classOf[java.lang.String])
  var customDelimiter: Option[String] = None

  @JsonProperty(defaultValue = "true")
  @JsonSchemaTitle("Header")
  @JsonPropertyDescription("whether the CSV file contains a header line")
  var hasHeader: Boolean = true

  fileTypeName = Option("CSV")

  @EqualsExclude
  @JsonIgnore
  var schema: Schema = _

  @JsonIgnore
  def appendSchema(newSchema: Schema): Unit = {
    // get the builder of current schema
    val builder = new Schema.Builder(schema)
    // for each attribute in passed schema, add it to builder
    newSchema.getAttributes.forEach(att => {
      if (!builder.isAttributeExists(att.getName)) {
        builder.add(att)
      }
    })
    // then build the new schema
    schema = builder.build()
  }

  @JsonIgnore
  def setFilePath(path: String): Unit = {
  filePath = Some(path)
  }

  @JsonIgnore
  def setSchema(s: Schema): Unit = {
    schema = s
  }

  @JsonIgnore
  def getSchema(): Schema = {
    if(schema == null) {
      inferSchema()
    }
    else {
      schema
    }
  }

  @throws[IOException]
  override def operatorExecutor(operatorSchemaInfo: OperatorSchemaInfo): OpExecConfig = {
    // fill in default values
    if (customDelimiter.get.isEmpty)
      customDelimiter = Option(",")

    filePath match {
      case Some(_) =>
        new ManyToOneOpExecConfig(operatorIdentifier, _ => new CSVScanSourceOpExec(this))
      case None =>
        throw new RuntimeException("File path is not provided.")
    }
  }

  /**
    * Infer Texera.Schema based on the top few lines of data.
    *
    * @return Texera.Schema build for this operator
    */
  @Override
  def inferSchema(): Schema = {
    if (customDelimiter.isEmpty) {
      return null
    }
    if (filePath.isEmpty) {
      return null
    }
    implicit object CustomFormat extends DefaultCSVFormat {
      override val delimiter: Char = customDelimiter.get.charAt(0)
    }
    var reader: CSVReader = CSVReader.open(filePath.get)(CustomFormat)
    val firstRow: Array[String] = reader.iterator.next().toArray
    reader.close()

    // reopen the file to read from the beginning
    reader = CSVReader.open(filePath.get)(CustomFormat)

    val startOffset = offset.getOrElse(0) + (if (hasHeader) 1 else 0)
    val endOffset =
      startOffset + limit.getOrElse(INFER_READ_LIMIT).min(INFER_READ_LIMIT)
    val attributeTypeList: Array[AttributeType] = inferSchemaFromRows(
      reader.iterator
        .slice(startOffset, endOffset)
        .map(seq => seq.toArray)
    )

    reader.close()

    // build schema based on inferred AttributeTypes
    Schema.newBuilder
      .add(
        firstRow.indices
          .map((i: Int) =>
            new Attribute(
              if (hasHeader) firstRow.apply(i) else "column-" + (i + 1),
              attributeTypeList.apply(i)
            )
          )
          .asJava
      )
      .build
  }

  override def constructSymbolicCondition(upstreamOperators: Array[OperatorDescriptor], z3Context: Context): Unit = {
    this.symbolicCondition = new SymbolicColumn(z3Context.mkTrue, z3Context.mkFalse, z3Context)
  }

  override def constructVariableConstraints(upstreamOperators: Array[OperatorDescriptor], z3Context: Context): Unit = {
    this.variableConstraints = z3Context.mkTrue
  }
  override def constructSymbolicColumns(upstreamOperators: Array[OperatorDescriptor], z3Context: Context): Unit = {
    if(upstreamOperators != null)
      this.symbolicColumns = upstreamOperators.head.getSymbolicColumns.clone()
    else{
        val constructedColumns = SymbolicColumn.constructSymbolicTuple(getSchema.getAttributes, z3Context)
        constructedColumns.forEach((tuple, column) => this.symbolicColumns.put(tuple, column))
    }
  }

  override def equals(that: Any): Boolean = {
    if (that == null || (getClass != that.getClass)) {
      false
    }
    else {
      val other = that.asInstanceOf[CSVScanSourceOpDesc]
      this.fileName.equals(other.fileName) && this.limit.equals(other.limit) && this.customDelimiter.equals(other.customDelimiter)
    }
  }
}
