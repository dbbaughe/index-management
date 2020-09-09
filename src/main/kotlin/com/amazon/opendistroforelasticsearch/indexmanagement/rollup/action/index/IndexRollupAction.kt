package com.amazon.opendistroforelasticsearch.indexmanagement.rollup.action.index

import org.elasticsearch.action.ActionType
import org.elasticsearch.action.index.IndexResponse

class IndexRollupAction private constructor() : ActionType<IndexResponse>(NAME, ::IndexResponse) {
    companion object {
        val INSTANCE = IndexRollupAction()
        val NAME = "cluster:admin/indexmanagement/rollup/index"
    }
}