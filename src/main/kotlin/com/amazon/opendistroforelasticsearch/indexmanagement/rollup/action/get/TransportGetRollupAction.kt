package com.amazon.opendistroforelasticsearch.indexmanagement.rollup.action.get

import com.amazon.opendistroforelasticsearch.indexmanagement.IndexManagementPlugin.Companion.INDEX_MANAGEMENT_INDEX
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.model.Rollup
import org.elasticsearch.ElasticsearchStatusException
import org.elasticsearch.action.ActionListener
import org.elasticsearch.action.get.GetRequest
import org.elasticsearch.action.get.GetResponse
import org.elasticsearch.action.support.ActionFilters
import org.elasticsearch.action.support.HandledTransportAction
import org.elasticsearch.client.Client
import org.elasticsearch.common.inject.Inject
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler
import org.elasticsearch.common.xcontent.NamedXContentRegistry
import org.elasticsearch.common.xcontent.XContentHelper
import org.elasticsearch.common.xcontent.XContentType
import org.elasticsearch.rest.RestStatus
import org.elasticsearch.tasks.Task
import org.elasticsearch.transport.TransportService
import java.lang.Exception

class TransportGetRollupAction @Inject constructor(
    transportService: TransportService,
    val client: Client,
    actionFilters: ActionFilters,
    val xContentRegistry: NamedXContentRegistry
) : HandledTransportAction<GetRollupRequest, GetRollupResponse> (
    GetRollupAction.NAME, transportService, actionFilters, ::GetRollupRequest
) {

    override fun doExecute(task: Task, request: GetRollupRequest, listener: ActionListener<GetRollupResponse>) {
        val getRequest = GetRequest(INDEX_MANAGEMENT_INDEX, request.id)
            //TODO version??
            .version(request.version)
            .fetchSourceContext(request.srcContext)

        client.get(getRequest, object : ActionListener<GetResponse> {
            override fun onResponse(response: GetResponse) {
                if (!response.isExists) {
                    return listener.onFailure(ElasticsearchStatusException("Rollup not found", RestStatus.NOT_FOUND))
                }

                var rollup: Rollup? = null
                if (!response.isSourceEmpty) {
                    XContentHelper.createParser(xContentRegistry, LoggingDeprecationHandler.INSTANCE,
                    response.sourceAsBytesRef, XContentType.JSON).use { xcp ->
                        rollup = Rollup.parseWithType(xcp, response.id, response.seqNo, response.primaryTerm)
                    }
                }

                listener.onResponse(GetRollupResponse(response.id, response.version, response.seqNo, response.primaryTerm, RestStatus.OK, rollup))
            }

            override fun onFailure(e: Exception) {
                listener.onFailure(e)
            }
        })
    }
}