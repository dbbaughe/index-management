/*
 *
 *  * Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License").
 *  * You may not use this file except in compliance with the License.
 *  * A copy of the License is located at
 *  *
 *  * http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * or in the "license" file accompanying this file. This file is distributed
 *  * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 *  * express or implied. See the License for the specific language governing
 *  * permissions and limitations under the License.
 *
 */

package com.amazon.opendistroforelasticsearch.indexmanagement.rollup.action.mapping

import com.amazon.opendistroforelasticsearch.indexmanagement.IndexManagementPlugin
import com.amazon.opendistroforelasticsearch.indexmanagement.elasticapi.string
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.util.XCONTENT_WITHOUT_TYPE
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.RollupMapperService
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.model.Rollup
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.util.getRollupJobs
import com.amazon.opendistroforelasticsearch.indexmanagement.util.IndexUtils
import com.amazon.opendistroforelasticsearch.indexmanagement.util.IndexUtils.Companion._META
import com.amazon.opendistroforelasticsearch.indexmanagement.util._DOC
import org.apache.logging.log4j.LogManager
import org.elasticsearch.action.ActionListener
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest
import org.elasticsearch.action.support.ActionFilters
import org.elasticsearch.action.support.master.AcknowledgedResponse
import org.elasticsearch.action.support.master.TransportMasterNodeAction
import org.elasticsearch.client.Client
import org.elasticsearch.cluster.AckedClusterStateUpdateTask
import org.elasticsearch.cluster.ClusterState
import org.elasticsearch.cluster.block.ClusterBlockException
import org.elasticsearch.cluster.block.ClusterBlockLevel
import org.elasticsearch.cluster.metadata.IndexMetadata
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver
import org.elasticsearch.cluster.metadata.MappingMetadata
import org.elasticsearch.cluster.metadata.Metadata
import org.elasticsearch.cluster.service.ClusterService
import org.elasticsearch.common.compress.CompressedXContent
import org.elasticsearch.common.inject.Inject
import org.elasticsearch.common.io.stream.StreamInput
import org.elasticsearch.common.io.stream.Writeable
import org.elasticsearch.common.xcontent.XContentBuilder
import org.elasticsearch.common.xcontent.XContentFactory
import org.elasticsearch.index.mapper.DocumentMapper
import org.elasticsearch.index.mapper.MapperService
import org.elasticsearch.index.mapper.MapperService.MergeReason
import org.elasticsearch.indices.IndicesService
import org.elasticsearch.threadpool.ThreadPool
import org.elasticsearch.transport.TransportService
import java.io.IOException
import java.lang.Exception

class TransportUpdateRollupMappingAction @Inject constructor(
    threadPool: ThreadPool,
    clusterService: ClusterService,
    transportService: TransportService,
    actionFilters: ActionFilters,
    indexNameExpressionResolver: IndexNameExpressionResolver,
    val indicesService: IndicesService,
    val client: Client
) : TransportMasterNodeAction<UpdateRollupMappingRequest, AcknowledgedResponse>(
    UpdateRollupMappingAction.INSTANCE.name(),
    transportService,
    clusterService,
    threadPool,
    actionFilters,
    Writeable.Reader { UpdateRollupMappingRequest(it) },
    indexNameExpressionResolver
) {

    private val log = LogManager.getLogger(javaClass)

    override fun checkBlock(request: UpdateRollupMappingRequest, state: ClusterState): ClusterBlockException? {
        return state.blocks.indicesBlockedException(ClusterBlockLevel.METADATA_WRITE, arrayOf(request.rollup.targetIndex))
    }

    override fun masterOperation(
        request: UpdateRollupMappingRequest,
        state: ClusterState,
        listener: ActionListener<AcknowledgedResponse>
    ) {
        val index = state.metadata.index(request.rollup.targetIndex)
        if (index == null) {
            log.debug("Could not find index [$index]")
            return listener.onFailure(IllegalStateException("Could not find index [$index]"))
        }
        val mappings = index.mapping()
        if (mappings == null) {
            log.debug("Could not find mapping for index [$index]")
            return listener.onFailure(IllegalStateException("Could not find mapping for index [$index]"))
        }
        val source = mappings.sourceAsMap
        if (source == null) {
            log.debug("Could not find source for index mapping [$index]")
            return listener.onFailure(IllegalStateException("Could not find source for index mapping [$index]"))
        }
        log.info("source is $source")
        val meta = source[_META]
        if (meta == null) {
            log.debug("Could not find meta mappings for index [$index]")
            return listener.onFailure(IllegalStateException("Could not find meta mappings for index [$index]"))
        }
        log.info("meta is $meta")
        val rollups = (meta as Map<*, *>)["rollups"]
        if (rollups == null) {
            log.debug("Could not find meta rollup mappings for index [$index]")
            return listener.onFailure(IllegalStateException("Could not find meta rollup mappings for index [$index]"))
        }
        if ((rollups as Map<*, *>).containsKey(request.rollup.id)) {
            log.debug("Meta rollup mappings already contain rollup ${request.rollup.id} for index [$index]")
            return listener.onFailure(IllegalStateException("Meta rollup mappings already contain rollup ${request.rollup.id} for index [$index]"))
        }

        request.rollup.toXContent(XContentFactory.jsonBuilder(), XCONTENT_WITHOUT_TYPE)

        // TODO: What does it mean if the rollup job is already in here?
        // We do not let you update it?

        // TODO: Does schema_version get overwritten?
        val putMappingRequest = PutMappingRequest(request.rollup.targetIndex).type(_DOC).source(meta)
        client.admin().indices().putMapping(putMappingRequest, object : ActionListener<AcknowledgedResponse> {
            override fun onResponse(response: AcknowledgedResponse) {
                listener.onResponse(response)
            }

            override fun onFailure(e: Exception) {
                listener.onFailure(e)
            }
        })
    }

    override fun read(sin: StreamInput): AcknowledgedResponse = AcknowledgedResponse(sin)

    override fun executor(): String = ThreadPool.Names.SAME

    @Throws(IOException::class)
    private fun partialRollupsMappingBuilder(rollupJobs: List<Rollup>): XContentBuilder {
        // TODO: This is dumping *everything* of rollup into the meta and we only want a slimmed down version of it
        // TODO: If we change to slimmed down version then we need a slimmed down version of parsing
        return XContentFactory.jsonBuilder()
            .startObject()
                .startObject(_META)
                    .startObject(RollupMapperService.ROLLUPS)
                        .apply {
                            rollupJobs.forEach {
                                this.field(it.id, it, XCONTENT_WITHOUT_TYPE)
                            }
                        }
                    .endObject()
                .endObject()
            .endObject()
    }
}
