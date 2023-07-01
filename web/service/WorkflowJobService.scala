package edu.uci.ics.texera.web.service

import com.twitter.util.{Await, Duration, Future}
import com.typesafe.scalalogging.LazyLogging
import edu.uci.ics.amber.engine.architecture.controller.promisehandlers.ModifyLogicHandler.ModifyLogic
import edu.uci.ics.amber.engine.architecture.controller.promisehandlers.StartWorkflowHandler.StartWorkflow
import edu.uci.ics.amber.engine.architecture.controller.{ControllerConfig, Workflow}
import edu.uci.ics.amber.engine.common.AmberUtils
import edu.uci.ics.amber.engine.common.client.AmberClient
import edu.uci.ics.amber.engine.common.virtualidentity.WorkflowIdentity
import edu.uci.ics.texera.web.model.websocket.request.{ModifyLogicRequest, WorkflowExecuteRequest}
import edu.uci.ics.texera.web.resource.dashboard.workflow.WorkflowVersionResource.{getLatestVersion, getWorkflowByVersion}
import edu.uci.ics.texera.web.service.WorkflowJobService.DELTA_SEMANTIC_REASONING_FLAG
import edu.uci.ics.texera.web.storage.JobStateStore
import edu.uci.ics.texera.web.workflowruntimestate.WorkflowAggregatedState.{READY, RUNNING}
import edu.uci.ics.texera.web.{SubscriptionManager, TexeraWebApplication, WebsocketInput}
import edu.uci.ics.texera.workflow.common.WorkflowContext
import edu.uci.ics.texera.workflow.common.workflow.WorkflowCompiler.ConstraintViolationException
import edu.uci.ics.texera.workflow.common.workflow.WorkflowInfo.toJgraphtDAG
import edu.uci.ics.texera.workflow.common.workflow.drove.{Drove, DroveBaseline, DrovePlus, DrovePlusPlus, Equitas}
import edu.uci.ics.texera.workflow.common.workflow.raven.Raven
import edu.uci.ics.texera.workflow.common.workflow.recycler.Recycler
import edu.uci.ics.texera.workflow.common.workflow.{DBWorkflowToLogicalPlan, WorkflowCompiler, WorkflowInfo, WorkflowRewriter}
import org.jooq.types.UInteger

import scala.collection.JavaConversions._

object WorkflowJobService {
  private final val DELTA_SEMANTIC_REASONING_FLAG: Boolean =
    AmberUtils.amberConfig.getBoolean("workflow-executions.delta-semantic-reasoning-flag")
}
class WorkflowJobService(
    workflowContext: WorkflowContext,
    wsInput: WebsocketInput,
    operatorCache: WorkflowCacheService,
    resultService: JobResultService,
    request: WorkflowExecuteRequest,
    errorHandler: Throwable => Unit
) extends SubscriptionManager
    with LazyLogging {

  val stateStore = new JobStateStore()
  val workflowInfo: WorkflowInfo = createWorkflowInfo()
  val workflowCompiler: WorkflowCompiler = createWorkflowCompiler(workflowInfo)
  val workflow: Workflow = workflowCompiler.amberWorkflow(
    WorkflowIdentity(workflowContext.jobId),
    resultService.opResultStorage
  )

  // Runtime starts from here:
  var client: AmberClient =
    TexeraWebApplication.createAmberRuntime(
      workflow,
      ControllerConfig.default,
      errorHandler
    )
  val jobBreakpointService = new JobBreakpointService(client, stateStore)
  val jobStatsService = new JobStatsService(client, stateStore)
  val jobRuntimeService =
    new JobRuntimeService(client, stateStore, wsInput, jobBreakpointService)
  val jobPythonService =
    new JobPythonService(client, stateStore, wsInput, jobBreakpointService)

  addSubscription(wsInput.subscribe((req: ModifyLogicRequest, uidOpt) => {
    workflowCompiler.initOperator(req.operator)
    client.sendAsync(ModifyLogic(req.operator))
  }))

  workflowContext.executionID = -1 // for every new execution,
  // reset it so that the value doesn't carry over across executions
  def startWorkflow(): Unit = {
    for (pair <- workflowInfo.breakpoints) {
      Await.result(
        jobBreakpointService.addBreakpoint(pair.operatorID, pair.breakpoint),
        Duration.fromSeconds(10)
      )
    }
    resultService.attachToJob(stateStore, workflowInfo, client)
    if (WorkflowService.userSystemEnabled) {
      // get previous executed versions of the same workflow to reason if the results are going to be the same
      val previousExecutedVersionsIDs:List[UInteger] = ExecutionsMetadataPersistService.getLatestExecutedVersionsOfWorkflow(workflowContext.wId)
      workflowContext.executionID =
        ExecutionsMetadataPersistService.insertNewExecution(workflowContext.wId)
      if(DELTA_SEMANTIC_REASONING_FLAG) {
      Future {
        /** RECYCLER pipeline
         * 1. match this DAG with historical DAG for exact match if there
         * is a match, rewrite it to reuse previous results
         */
//        val recycler: Recycler = TexeraWebApplication.getRecyclerGraph()
        var t0 = System.currentTimeMillis()
//        // either match if not matched update the graph (so
//        // function either returns set of sinks if matched or empty if not matched)
//        recycler.matchAndUpdate(workflowInfo.toDAG)
//        System.out.println("Recycler " + (System.currentTimeMillis() - t0))
//        System.out.println("=====================================")

        /** RAVEN pipeline
         * 1. based on ranking (retrieve those that are useful and sorted then run the below loop)
         * DROVE+ is the baseline and DROVE++ is the modified to include equivalence classes
         */
          val raven: Raven = TexeraWebApplication.getRaven()
        val cachedWindows = TexeraWebApplication.getCachedWindows()
//        t0 = System.currentTimeMillis()
//        val rankedVersions = raven.getRankedVersions(UInteger.valueOf(workflowContext.wId), getLatestVersion(UInteger.valueOf(workflowContext.wId)), workflowInfo.toDAG)
//        System.out.println("Ranking " + (System.currentTimeMillis() - t0))
//        System.out.println("=====================================")
        var allmatched: Boolean = false
        for (versionID <- previousExecutedVersionsIDs) {
          if (!allmatched) {
            // only loop when there are still some sinks not matched
            System.out.println("VERSION " + versionID)
            val previousWorkflow = getWorkflowByVersion(UInteger.valueOf(workflowContext.wId), versionID)
            // TODO pass context and executionID to set it for each operator
            // convert workflow format from DB format to engine Logical DAG
            val workflowCasting: DBWorkflowToLogicalPlan = new DBWorkflowToLogicalPlan(previousWorkflow.getContent, workflowContext)
            workflowCasting.createLogicalPlan()
            t0 = System.currentTimeMillis()
            val equitas: Equitas = new Equitas()
            //          val drove: Drove = new DroveBaseline(equitas, workflowCasting.getWorkflowLogicalPlan().toDAG, workflowInfo.toDAG)
            //          val unchangedSinks = drove.getEquivalentSinks()
            //          val droveplus: Drove = new DrovePlus(equitas, workflowCasting.getWorkflowLogicalPlan().toDAG, workflowInfo.toDAG)
            //          val unchanged = droveplus.getEquivalentSinks()
            val droveplusplus: Drove = new DrovePlusPlus(cachedWindows, equitas, workflowCasting.getWorkflowLogicalPlan().toDAG, workflowInfo.toDAG)
            val unchangedplus = droveplusplus.getEquivalentSinks()
            System.out.println("Veer " + (System.currentTimeMillis() - t0))
            System.out.println("=====================================")
            if(unchangedplus.containsAll(workflowInfo.toDAG.sinkOperators)) {
              allmatched = true
            }
          }
        }
      }
      }
    }
    stateStore.jobMetadataStore.updateState(jobInfo =>
      jobInfo.withState(READY).withEid(workflowContext.executionID).withError(null)
    )
    client.sendAsyncWithCallback[Unit](
      StartWorkflow(),
      _ => stateStore.jobMetadataStore.updateState(jobInfo => jobInfo.withState(RUNNING))
    )
  }

  private[this] def createWorkflowInfo(): WorkflowInfo = {
    var workflowInfo = WorkflowInfo(request.operators, request.links, request.breakpoints)
    if (WorkflowCacheService.isAvailable) {
      workflowInfo.cachedOperatorIds = request.cachedOperatorIds
      logger.debug(
        s"Cached operators: ${operatorCache.cachedOperators} with ${request.cachedOperatorIds}"
      )
      val workflowRewriter = new WorkflowRewriter(
        workflowInfo,
        operatorCache.cachedOperators,
        operatorCache.cacheSourceOperators,
        operatorCache.cacheSinkOperators,
        operatorCache.operatorRecord,
        resultService.opResultStorage
      )
      val newWorkflowInfo = workflowRewriter.rewrite
      val oldWorkflowInfo = workflowInfo
      workflowInfo = newWorkflowInfo
      workflowInfo.cachedOperatorIds = oldWorkflowInfo.cachedOperatorIds
      logger.info(
        s"Rewrite the original workflow: ${toJgraphtDAG(oldWorkflowInfo)} to be: ${toJgraphtDAG(workflowInfo)}"
      )
    }
    workflowInfo
  }

  private[this] def createWorkflowCompiler(
      workflowInfo: WorkflowInfo
  ): WorkflowCompiler = {
    val compiler = new WorkflowCompiler(workflowInfo, workflowContext)
    val violations = compiler.validate
    if (violations.nonEmpty) {
      throw new ConstraintViolationException(violations)
    }
    compiler
  }

  override def unsubscribeAll(): Unit = {
    super.unsubscribeAll()
    jobBreakpointService.unsubscribeAll()
    jobRuntimeService.unsubscribeAll()
    jobPythonService.unsubscribeAll()
    jobStatsService.unsubscribeAll()
  }

}
