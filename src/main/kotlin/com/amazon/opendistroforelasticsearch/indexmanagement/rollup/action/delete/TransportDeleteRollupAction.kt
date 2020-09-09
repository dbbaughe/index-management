package com.amazon.opendistroforelasticsearch.indexmanagement.rollup.action.delete

import com.amazon.opendistroforelasticsearch.indexmanagement.IndexManagementPlugin.Companion.INDEX_MANAGEMENT_INDEX
import org.elasticsearch.action.ActionListener
import org.elasticsearch.action.delete.DeleteRequest
import org.elasticsearch.action.delete.DeleteResponse
import org.elasticsearch.action.support.ActionFilters
import org.elasticsearch.action.support.HandledTransportAction
import org.elasticsearch.client.Client
import org.elasticsearch.common.inject.Inject
import org.elasticsearch.tasks.Task
import org.elasticsearch.transport.TransportService

class TransportDeleteRollupAction @Inject constructor(
    transportService: TransportService,
    val client: Client,
    actionFilters: ActionFilters
) : HandledTransportAction<DeleteRollupRequest, DeleteResponse>(
    DeleteRollupAction.NAME, transportService, actionFilters, ::DeleteRollupRequest
) {

    override fun doExecute(task: Task, request: DeleteRollupRequest, actionListener: ActionListener<DeleteResponse>) {
        val deleteRequest = DeleteRequest(INDEX_MANAGEMENT_INDEX, request.rollupID())
            .setRefreshPolicy(request.refreshPolicy)

        client.delete(deleteRequest, object : ActionListener<DeleteResponse> {
            override fun onResponse(response: DeleteResponse) {
                actionListener.onResponse(response)
            }

            override fun onFailure(t: Exception) {
                actionListener.onFailure(t)
            }
        })
    }
}