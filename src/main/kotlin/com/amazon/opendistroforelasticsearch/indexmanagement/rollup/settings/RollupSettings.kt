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

package com.amazon.opendistroforelasticsearch.indexmanagement.rollup.settings

import org.elasticsearch.common.settings.Setting

class RollupSettings {
    companion object {
        const val DEFAULT_ROLLUP_ENABLED = true

        val ROLLUP_ENABLED: Setting<Boolean> = Setting.boolSetting(
            "opendistro.rollup.enabled",
            DEFAULT_ROLLUP_ENABLED,
            Setting.Property.NodeScope,
            Setting.Property.Dynamic
        )

        val ROLLUP_INDEX: Setting<Boolean> = Setting.boolSetting(
            "index.opendistro.rollup_index",
            false,
            Setting.Property.IndexScope,
            Setting.Property.InternalIndex
        )
    }
}
