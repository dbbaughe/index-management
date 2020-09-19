/*
 * Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package com.amazon.opendistroforelasticsearch.indexmanagement

import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.IndexStateManagementHistory
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.ManagedIndexCoordinator
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.ManagedIndexRunner
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.transport.action.updateindexmetadata.TransportUpdateManagedIndexMetaDataAction
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.transport.action.updateindexmetadata.UpdateManagedIndexMetaDataAction
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.model.ManagedIndexConfig
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.model.Policy
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.resthandler.RestAddPolicyAction
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.resthandler.RestChangePolicyAction
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.resthandler.RestDeletePolicyAction
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.resthandler.RestExplainAction
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.resthandler.RestGetPolicyAction
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.resthandler.RestIndexPolicyAction
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.resthandler.RestRemovePolicyAction
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.resthandler.RestRetryFailedManagedIndexAction
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.settings.ManagedIndexSettings
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.RollupActionFilter
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.RollupInterceptor
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.RollupRunner
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.RollupSearchListener
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.action.delete.DeleteRollupAction
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.action.delete.TransportDeleteRollupAction
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.action.get.GetRollupAction
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.action.get.TransportGetRollupAction
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.action.index.IndexRollupAction
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.action.index.TransportIndexRollupAction
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.action.start.StartRollupAction
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.action.start.TransportStartRollupAction
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.action.stop.StopRollupAction
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.action.stop.TransportStopRollupAction
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.model.Rollup
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.resthandler.RestDeleteRollupAction
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.resthandler.RestGetRollupAction
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.resthandler.RestIndexRollupAction
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.resthandler.RestStartRollupAction
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.resthandler.RestStopRollupAction
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.settings.RollupSettings
import com.amazon.opendistroforelasticsearch.jobscheduler.spi.JobSchedulerExtension
import com.amazon.opendistroforelasticsearch.jobscheduler.spi.ScheduledJobParser
import com.amazon.opendistroforelasticsearch.jobscheduler.spi.ScheduledJobRunner
import org.apache.logging.log4j.LogManager
import org.apache.lucene.queryparser.xml.builders.MatchAllDocsQueryBuilder
import org.apache.lucene.search.MatchAllDocsQuery
import org.elasticsearch.ElasticsearchException
import org.elasticsearch.ElasticsearchStatusException
import org.elasticsearch.action.ActionListener
import org.elasticsearch.action.ActionRequest
import org.elasticsearch.action.ActionResponse
import org.elasticsearch.action.bulk.BulkRequest
import org.elasticsearch.action.search.MultiSearchRequest
import org.elasticsearch.action.search.SearchRequest
import org.elasticsearch.action.search.SearchTask
import org.elasticsearch.action.support.ActionFilter
import org.elasticsearch.action.support.ActionFilterChain
import org.elasticsearch.client.Client
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver
import org.elasticsearch.cluster.node.DiscoveryNodes
import org.elasticsearch.cluster.service.ClusterService
import org.elasticsearch.common.io.stream.NamedWriteableRegistry
import org.elasticsearch.common.network.NetworkService
import org.elasticsearch.common.settings.ClusterSettings
import org.elasticsearch.common.settings.IndexScopedSettings
import org.elasticsearch.common.settings.Setting
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.common.settings.SettingsFilter
import org.elasticsearch.common.util.BigArrays
import org.elasticsearch.common.util.PageCacheRecycler
import org.elasticsearch.common.util.concurrent.ThreadContext
import org.elasticsearch.common.xcontent.NamedXContentRegistry
import org.elasticsearch.common.xcontent.XContentParser.Token
import org.elasticsearch.common.xcontent.XContentParserUtils.ensureExpectedToken
import org.elasticsearch.env.Environment
import org.elasticsearch.env.NodeEnvironment
import org.elasticsearch.http.HttpServerTransport
import org.elasticsearch.index.IndexModule
import org.elasticsearch.index.query.MatchAllQueryBuilder
import org.elasticsearch.index.query.MatchNoneQueryBuilder
import org.elasticsearch.index.query.QueryBuilder
import org.elasticsearch.indices.breaker.CircuitBreakerService
import org.elasticsearch.plugins.ActionPlugin
import org.elasticsearch.plugins.NetworkPlugin
import org.elasticsearch.plugins.Plugin
import org.elasticsearch.plugins.SearchPlugin
import org.elasticsearch.repositories.RepositoriesService
import org.elasticsearch.rest.RestController
import org.elasticsearch.rest.RestHandler
import org.elasticsearch.rest.RestStatus
import org.elasticsearch.script.ScriptService
import org.elasticsearch.search.builder.SearchSourceBuilder
import org.elasticsearch.tasks.Task
import org.elasticsearch.threadpool.ThreadPool
import org.elasticsearch.transport.TransportInterceptor
import org.elasticsearch.watcher.ResourceWatcherService
import java.util.function.Supplier

internal class IndexManagementPlugin : JobSchedulerExtension, SearchPlugin, ActionPlugin, NetworkPlugin, Plugin() {

    private val logger = LogManager.getLogger(javaClass)
    lateinit var indexManagementIndices: IndexManagementIndices
    lateinit var clusterService: ClusterService
    lateinit var rollupSearchListener: RollupSearchListener
    lateinit var rollupInterceptor: RollupInterceptor
    lateinit var indexNameExpressionResolver: IndexNameExpressionResolver

    companion object {
        const val PLUGIN_NAME = "opendistro-im"
        const val OPEN_DISTRO_BASE_URI = "/_opendistro"
        const val ISM_BASE_URI = "$OPEN_DISTRO_BASE_URI/_ism"
        const val ROLLUP_BASE_URI = "$OPEN_DISTRO_BASE_URI/_rollup"
        const val POLICY_BASE_URI = "$ISM_BASE_URI/policies"
        const val ROLLUP_JOBS_BASE_URI = "$ROLLUP_BASE_URI/jobs"
        const val INDEX_MANAGEMENT_INDEX = ".opendistro-ism-config"
        const val INDEX_MANAGEMENT_JOB_TYPE = "opendistro-index-management"
        const val INDEX_STATE_MANAGEMENT_HISTORY_TYPE = "managed_index_meta_data"
    }

    override fun getJobIndex(): String = INDEX_MANAGEMENT_INDEX

    override fun getJobType(): String = INDEX_MANAGEMENT_JOB_TYPE

    override fun getJobRunner(): ScheduledJobRunner = IndexManagementRunner

    override fun getJobParser(): ScheduledJobParser {
        return ScheduledJobParser { xcp, id, jobDocVersion ->
            ensureExpectedToken(Token.START_OBJECT, xcp.nextToken(), xcp::getTokenLocation)
            while (xcp.nextToken() != Token.END_OBJECT) {
                val fieldName = xcp.currentName()
                xcp.nextToken()

                when (fieldName) {
                    ManagedIndexConfig.MANAGED_INDEX_TYPE -> {
                        return@ScheduledJobParser ManagedIndexConfig.parse(xcp, id, jobDocVersion.seqNo, jobDocVersion.primaryTerm)
                    }
                    Policy.POLICY_TYPE -> {
                        return@ScheduledJobParser null
                    }
                    Rollup.ROLLUP_TYPE -> {
                        return@ScheduledJobParser Rollup.parse(xcp, id, jobDocVersion.seqNo, jobDocVersion.primaryTerm)
                    }
                    else -> {
                        logger.info("Unsupported document was indexed in $INDEX_MANAGEMENT_INDEX with type: $fieldName")
                    }
                }
            }
            return@ScheduledJobParser null
        }
    }

    override fun getRestHandlers(
        settings: Settings,
        restController: RestController,
        clusterSettings: ClusterSettings,
        indexScopedSettings: IndexScopedSettings,
        settingsFilter: SettingsFilter,
        indexNameExpressionResolver: IndexNameExpressionResolver,
        nodesInCluster: Supplier<DiscoveryNodes>
    ): List<RestHandler> {
        return listOf(
            RestIndexPolicyAction(settings, clusterService, indexManagementIndices),
            RestGetPolicyAction(),
            RestDeletePolicyAction(),
            RestExplainAction(),
            RestRetryFailedManagedIndexAction(),
            RestAddPolicyAction(),
            RestRemovePolicyAction(),
            RestChangePolicyAction(clusterService),
            RestIndexRollupAction(),
            RestGetRollupAction(),
            RestDeleteRollupAction(),
            RestStartRollupAction(),
            RestStopRollupAction()
        )
    }

    override fun createComponents(
        client: Client,
        clusterService: ClusterService,
        threadPool: ThreadPool,
        resourceWatcherService: ResourceWatcherService,
        scriptService: ScriptService,
        xContentRegistry: NamedXContentRegistry,
        environment: Environment,
        nodeEnvironment: NodeEnvironment,
        namedWriteableRegistry: NamedWriteableRegistry,
        indexNameExpressionResolver: IndexNameExpressionResolver,
        repositoriesServiceSupplier: Supplier<RepositoriesService>
    ): Collection<Any> {
        val settings = environment.settings()
        this.clusterService = clusterService
        val managedIndexRunner = ManagedIndexRunner
            .registerClient(client)
            .registerClusterService(clusterService)
            .registerNamedXContentRegistry(xContentRegistry)
            .registerScriptService(scriptService)
            .registerSettings(settings)
            .registerConsumers() // registerConsumers must happen after registerSettings/clusterService
        val rollupRunner = RollupRunner
            .registerClient(client)
            .registerClusterService(clusterService)
            .registerNamedXContentRegistry(xContentRegistry)
            .registerScriptService(scriptService)
            .registerSettings(settings)
            .registerConsumers()
        rollupSearchListener = RollupSearchListener(clusterService, indexNameExpressionResolver)
        rollupInterceptor = RollupInterceptor()
        this.indexNameExpressionResolver = indexNameExpressionResolver
        indexManagementIndices = IndexManagementIndices(client.admin().indices(), clusterService)
        val indexStateManagementHistory =
            IndexStateManagementHistory(
                settings,
                client,
                threadPool,
                clusterService,
                indexManagementIndices
            )

        val managedIndexCoordinator = ManagedIndexCoordinator(environment.settings(),
            client, clusterService, threadPool, indexManagementIndices)

        return listOf(managedIndexRunner, indexManagementIndices, managedIndexCoordinator, indexStateManagementHistory)
    }

    override fun getSettings(): List<Setting<*>> {
        return listOf(
            ManagedIndexSettings.HISTORY_ENABLED,
            ManagedIndexSettings.HISTORY_INDEX_MAX_AGE,
            ManagedIndexSettings.HISTORY_MAX_DOCS,
            ManagedIndexSettings.HISTORY_RETENTION_PERIOD,
            ManagedIndexSettings.HISTORY_ROLLOVER_CHECK_PERIOD,
            ManagedIndexSettings.POLICY_ID,
            ManagedIndexSettings.ROLLOVER_ALIAS,
            ManagedIndexSettings.INDEX_STATE_MANAGEMENT_ENABLED,
            ManagedIndexSettings.JOB_INTERVAL,
            ManagedIndexSettings.SWEEP_PERIOD,
            ManagedIndexSettings.COORDINATOR_BACKOFF_COUNT,
            ManagedIndexSettings.COORDINATOR_BACKOFF_MILLIS,
            ManagedIndexSettings.ALLOW_LIST,
            RollupSettings.ROLLUP_ENABLED,
            RollupSettings.ROLLUP_INDEX,
            RollupSettings.ROLLUP_SEARCH_ENABLED,
            RollupSettings.ROLLUP_INGEST_BACKOFF_COUNT,
            RollupSettings.ROLLUP_INGEST_BACKOFF_MILLIS
        )
    }

    override fun getActions(): List<ActionPlugin.ActionHandler<out ActionRequest, out ActionResponse>> {
        return listOf(
            ActionPlugin.ActionHandler(UpdateManagedIndexMetaDataAction.INSTANCE, TransportUpdateManagedIndexMetaDataAction::class.java),
            ActionPlugin.ActionHandler(DeleteRollupAction.INSTANCE, TransportDeleteRollupAction::class.java),
            ActionPlugin.ActionHandler(IndexRollupAction.INSTANCE, TransportIndexRollupAction::class.java),
            ActionPlugin.ActionHandler(GetRollupAction.INSTANCE, TransportGetRollupAction::class.java),
            ActionPlugin.ActionHandler(StartRollupAction.INSTANCE, TransportStartRollupAction::class.java),
            ActionPlugin.ActionHandler(StopRollupAction.INSTANCE, TransportStopRollupAction::class.java)
        )
    }

    override fun onIndexModule(indexModule: IndexModule) {
        val rollupIndex = RollupSettings.ROLLUP_INDEX.get(indexModule.settings)
        if (rollupIndex) {
             indexModule.addSearchOperationListener(rollupSearchListener)
        }
    }

//    override fun getTransportInterceptors(namedWriteableRegistry: NamedWriteableRegistry, threadContext: ThreadContext): List<TransportInterceptor> {
//        return listOf(rollupInterceptor)
//    }

//    override fun getActionFilters(): List<ActionFilter> {
//        return listOf(RollupActionFilter(clusterService, indexNameExpressionResolver))
//    }
}
