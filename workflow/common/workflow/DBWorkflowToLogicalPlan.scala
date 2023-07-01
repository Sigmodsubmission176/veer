package edu.uci.ics.texera.workflow.common.workflow

import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import edu.uci.ics.texera.Utils.objectMapper
import edu.uci.ics.texera.workflow.common.WorkflowContext
import edu.uci.ics.texera.workflow.common.operators.OperatorDescriptor
import edu.uci.ics.texera.workflow.common.workflow.DBWorkflowToLogicalPlan.{flattenOperatorProperties, getAllEnabledBreakpoints}

import scala.collection.convert.ImplicitConversions.`iterable AsScalaIterable`
import scala.collection.mutable

object DBWorkflowToLogicalPlan {

  def flattenOperatorProperties(operatorNode: JsonNode, workflowContext: WorkflowContext) : OperatorDescriptor = {
    val objectNode = objectMapper.createObjectNode()
    objectNode.put("operatorID", operatorNode.get("operatorID").asText())
    objectNode.put("operatorType", operatorNode.get("operatorType").asText())
    val opPropertiesIter = operatorNode.get("operatorProperties").fields()
    while(opPropertiesIter.hasNext) {
      val field = opPropertiesIter.next()
      objectNode.put(field.getKey, field.getValue)
    }
    val op: OperatorDescriptor = objectMapper.readValue(objectMapper.writeValueAsString(objectNode), classOf[OperatorDescriptor])
    op.setContext(workflowContext)
    op
  }

  def getAllEnabledBreakpoints(breakpoints: JsonNode): mutable.MutableList[BreakpointInfo] = {
    mutable.MutableList() // TODO map the breakpoints
  }
}

case class DBWorkflowToLogicalPlan(workflowContent: String, workflowContext: WorkflowContext) {
  var operators: mutable.MutableList[OperatorDescriptor] = mutable.MutableList()
  var links: mutable.MutableList[OperatorLink] = mutable.MutableList()
  var breakpoints: mutable.MutableList[BreakpointInfo] = mutable.MutableList()
  val content: String = workflowContent
  var disabledOperatorIDs: mutable.MutableList[String] = mutable.MutableList()

  def getAllEnabledOperators(operators: JsonNode): mutable.MutableList[OperatorDescriptor] = {
    var mappedOperators: mutable.MutableList[OperatorDescriptor] = mutable.MutableList()
    // filter out disabled
    var disabledFlag: Boolean = false
    // then insert its id, type, and flatten all other properties
    operators.foreach(op => {
      disabledFlag = false
      if(op.has("isDisabled")) {
        disabledFlag = op.get("isDisabled").asBoolean()
      }
      if(disabledFlag) {
        disabledOperatorIDs += op.get("operatorID").asText()
      } else {
        mappedOperators += flattenOperatorProperties(op, workflowContext)
      }
    })
    mappedOperators

  }

  def getAllEnabledLinks(links: JsonNode): mutable.MutableList[OperatorLink] = {
    var mappedLinks: mutable.MutableList[OperatorLink] = mutable.MutableList()
    // filter out disabled
    links.foreach(link => {
      val source = link.get("source")
      val target = link.get("target")
      val sourceID = source.get("operatorID").asText()
      val targetID = target.get("operatorID").asText()
      if(!disabledOperatorIDs.contains(sourceID) && !disabledOperatorIDs.contains(targetID)) {
        val linkNode = objectMapper.createObjectNode()
        val origin = objectMapper.createObjectNode()
        val sourceOrdinal = source.get("portID").asText()
        val targetOrdinal = target.get("portID").asText()
        origin.put("operatorID", sourceID)
        origin.put("portOrdinal", Integer.valueOf(sourceOrdinal.substring(sourceOrdinal.indexOf("output-")+7)))
        val destination = objectMapper.createObjectNode()
        destination.put("operatorID", targetID)
        destination.put("portOrdinal", Integer.valueOf(targetOrdinal.substring(targetOrdinal.indexOf("input-")+6)))
        linkNode.put("origin", origin)
        linkNode.put("destination", destination)
        mappedLinks += objectMapper.readValue(linkNode.toString, classOf[OperatorLink])
      }
    })
    mappedLinks
  }

  def createLogicalPlan(): Unit = {
    // parse the json tree
    val jsonMapper = new ObjectMapper()
    val jsonTree = jsonMapper.readTree(content)
    val operators = jsonTree.get("operators")
    this.operators = getAllEnabledOperators(operators)
    val links = jsonTree.get("links")
    this.links = getAllEnabledLinks(links)
    val breakpoints = jsonTree.get("breakpoints")
    this.breakpoints = getAllEnabledBreakpoints(breakpoints)

  }

  def getWorkflowLogicalPlan(): WorkflowInfo = {
    WorkflowInfo(operators, links, breakpoints)
  }
}
