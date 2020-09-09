package com.amazon.opendistroforelasticsearch.indexmanagement.rollup.action.get

import org.elasticsearch.action.ActionType

class GetRollupAction private constructor() : ActionType<GetRollupResponse>(NAME, ::GetRollupResponse) {
    companion object {
        val INSTANCE = GetRollupAction()
        val NAME = "cluster:admin/indexmanagement/rollup/get"
    }
}