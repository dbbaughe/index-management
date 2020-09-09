package com.amazon.opendistroforelasticsearch.indexmanagement.rollup.action.delete

import org.elasticsearch.action.ActionType
import org.elasticsearch.action.delete.DeleteResponse

class DeleteRollupAction private constructor() : ActionType<DeleteResponse>(NAME, ::DeleteResponse) {
    companion object {
        val INSTANCE = DeleteRollupAction()
        val NAME = "cluster:admin/indexmanagement/rollup/delete"
    }
}