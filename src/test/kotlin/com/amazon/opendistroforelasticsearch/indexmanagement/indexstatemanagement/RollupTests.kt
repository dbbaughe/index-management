package com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement

import com.amazon.opendistroforelasticsearch.indexmanagement.IndexManagementIndices
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.RollupRunner
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler
import org.elasticsearch.common.xcontent.NamedXContentRegistry
import org.elasticsearch.common.xcontent.ToXContent
import org.elasticsearch.common.xcontent.XContent
import org.elasticsearch.common.xcontent.XContentFactory
import org.elasticsearch.common.xcontent.XContentHelper
import org.elasticsearch.common.xcontent.XContentParser
import org.elasticsearch.common.xcontent.XContentParserUtils
import org.elasticsearch.common.xcontent.XContentType
import org.elasticsearch.test.ESTestCase

class RollupTests : ESTestCase() {

    fun `test mapping parsing`() {
        logger.info("mappings are ${IndexManagementIndices.rollupTargetMappings}")

        val xcp = XContentType.JSON.xContent().createParser(NamedXContentRegistry.EMPTY, LoggingDeprecationHandler.INSTANCE, IndexManagementIndices.rollupTargetMappings)
        XContentParserUtils.ensureExpectedToken(
            XContentParser.Token.START_OBJECT,
            xcp.nextToken(),
            xcp::getTokenLocation
        ) // start of mappings block
        while (xcp.nextToken() != XContentParser.Token.END_OBJECT) {
            val fieldName = xcp.currentName()
            xcp.nextToken()

            when (fieldName) {
                "_meta" -> {
                    logger.info("Field name is meta")
                    logger.info("Current token ${xcp.currentToken()}")
                    logger.info("Current name ${xcp.currentName()}")
                    XContentParserUtils.ensureExpectedToken(
                        XContentParser.Token.START_OBJECT,
                        xcp.currentToken(),
                        xcp::getTokenLocation
                    ) // start of mappings block
                    while (xcp.nextToken() != XContentParser.Token.END_OBJECT) {
                        val innerFieldName = xcp.currentName()
                        xcp.nextToken()

                        when (innerFieldName) {
                            "rollups" -> {
                                logger.info("Inner field name is rollups")
                            }
                            else -> xcp.skipChildren()
                        }
                    }
                }
                else -> xcp.skipChildren()
            }
        }

        //val intervalJsonString = managedIndexConfig.schedule.toXContent(XContentFactory.jsonBuilder(), ToXContent.EMPTY_PARAMS).string()
        //            val xcp = XContentType.JSON.xContent().createParser(xContentRegistry, LoggingDeprecationHandler.INSTANCE, intervalJsonString)
        //            ensureExpectedToken(Token.START_OBJECT, xcp.nextToken(), xcp::getTokenLocation) // start of schedule block
        //            ensureExpectedToken(Token.FIELD_NAME, xcp.nextToken(), xcp::getTokenLocation) // "interval"
        //            ensureExpectedToken(Token.START_OBJECT, xcp.nextToken(), xcp::getTokenLocation) // start of interval block
        //            var startTime: Long? = null
        //            while (xcp.nextToken() != Token.END_OBJECT) {
        //                val fieldName = xcp.currentName()
        //                xcp.nextToken()
        //                when (fieldName) {
        //                    "start_time" -> startTime = xcp.longValue()
        //                }
        //            }
        //            Instant.ofEpochMilli(requireNotNull(startTime))
        //        }
        //    }
        fail()
    }
}