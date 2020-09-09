package com.amazon.opendistroforelasticsearch.indexmanagement.rollup.action.index

import org.elasticsearch.action.ActionType

class IndexRollupAction private constructor() : ActionType<IndexRollupResponse>(NAME, ::IndexRollupResponse) {
    companion object {
        val INSTANCE = IndexRollupAction()
        val NAME = "cluster:admin/indexmanagement/rollup/index"
    }
}