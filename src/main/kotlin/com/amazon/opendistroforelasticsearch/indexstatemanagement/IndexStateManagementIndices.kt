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

package com.amazon.opendistroforelasticsearch.indexstatemanagement

import com.amazon.opendistroforelasticsearch.indexstatemanagement.IndexStateManagementPlugin.Companion.INDEX_STATE_MANAGEMENT_INDEX
import com.amazon.opendistroforelasticsearch.indexstatemanagement.IndexStateManagementPlugin.Companion.INDEX_STATE_MANAGEMENT_DOC_TYPE
import org.elasticsearch.action.ActionListener
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse
import org.elasticsearch.client.IndicesAdminClient
import org.elasticsearch.cluster.service.ClusterService
import org.elasticsearch.common.xcontent.XContentType

// TODO: Handle updating mappings on newer versions
// TODO: Handle Auditing indices

class IndexStateManagementIndices(
    private val client: IndicesAdminClient,
    private val clusterService: ClusterService
) {

    fun initIndexStateManagementIndex(actionListener: ActionListener<CreateIndexResponse>) {
        if (!indexStateManagementIndexExists()) {
            val indexRequest = CreateIndexRequest(INDEX_STATE_MANAGEMENT_INDEX)
                    .mapping(INDEX_STATE_MANAGEMENT_DOC_TYPE, indexStateManagementMappings(), XContentType.JSON)
            client.create(indexRequest, actionListener)
        }
    }

    fun indexStateManagementIndexExists(): Boolean {
        return clusterService.state().routingTable.hasIndex(INDEX_STATE_MANAGEMENT_INDEX)
    }

    private fun indexStateManagementMappings(): String {
        return javaClass.classLoader.getResource("mappings/opendistro-ism-config.json").readText()
    }
}