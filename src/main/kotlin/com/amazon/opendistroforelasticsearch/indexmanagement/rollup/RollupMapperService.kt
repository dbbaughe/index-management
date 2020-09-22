package com.amazon.opendistroforelasticsearch.indexmanagement.rollup

import com.amazon.opendistroforelasticsearch.indexmanagement.IndexManagementIndices
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.elasticapi.suspendUntil
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.util.XCONTENT_WITHOUT_TYPE
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.model.Rollup
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.settings.RollupSettings
import com.amazon.opendistroforelasticsearch.indexmanagement.util._DOC
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import org.apache.logging.log4j.LogManager
import org.elasticsearch.ResourceAlreadyExistsException
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest
import org.elasticsearch.action.support.master.AcknowledgedResponse
import org.elasticsearch.client.Client
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.common.xcontent.XContentFactory
import org.elasticsearch.common.xcontent.XContentType
import org.elasticsearch.transport.RemoteTransportException

class RollupMapperService(val client: Client) { // TODO: lease priveleged client
    private val logger = LogManager.getLogger(javaClass)

    // This creates the target index if it doesn't already exist
    // Should reject if the target index exists and is not a rolled up index
    // TODO: already existing rollup target index
    suspend fun createRollupTargetIndex(job: Rollup) {
        try {
            val request = CreateIndexRequest(job.targetIndex)
                .settings(Settings.builder().put(RollupSettings.ROLLUP_INDEX.key, true).build())
                .mapping(_DOC, IndexManagementIndices.rollupTargetMappings, XContentType.JSON)
                // TODO: perhaps we can do better than this for mappings...
                //  can we read in the actual mappings from the source index and use that?
                //  it'll have issues with metrics though, i.e. an int mapping with 3, 5, 6 added up and divided by 3 for avg is 14/3 = 4.6666
                //  what happens if the first indexing is an integer, i.e. 3 + 3 + 3 = 9/3 = 3 and it saves it as int
                //  and then the next is float and it fails or rounds it up?
                // what could be nice is if I do the optimization of mappings for all the dimensions and then I dont have to
                // rely on the *.date_histogram.* dynamic mapping and could directly
                // use the dimension field names as they were which means I dont even have to transform
                // any terms, date histograms, or histograms
            val response: CreateIndexResponse = client.admin().indices().suspendUntil { create(request, it) }
            logger.info("Response ${response.isAcknowledged}")
            // Test should not be able to put rollup metadata in non rollup index

            withContext(Dispatchers.IO) {
                // TODO: Clean up, can this just be combined with the initial create request?
                val builder = XContentFactory.jsonBuilder().startObject() // TODO: inapp blocking call?
                    .startObject("_meta")
                    .startObject("rollups")
                    .field(job.id, job, XCONTENT_WITHOUT_TYPE)
                    .endObject()
                    .endObject()
                    .endObject()
                val putMappingRequest = PutMappingRequest(job.targetIndex).type(_DOC)
                    .source(builder)

                // put mappings needs to ensure we dont overwrite an exisitng rollup job meta
                // probably can just get current mappings and parse all the existing job ids - if this job id already exists then ignore
                // should we let a person delete a job from the meta mappings of an index? they would have to make sure they deleted the data too
                val putResponse: AcknowledgedResponse = client.admin().indices().suspendUntil { putMapping(putMappingRequest, it) }
                logger.info("PutResponse: ${putResponse.isAcknowledged}")
            }
        } catch (e: RemoteTransportException) {
            logger.info("RemoteTransportException") // TODO: handle resource already exists too
        } catch (e: ResourceAlreadyExistsException) {
            logger.info("Index exists already") // TODO
        } catch (e: Exception) {
            logger.error(e) // TODO
        }
    }
}

// TODO: Clean up
//{ meta: { rollup: { <id>: <job> } } }
//                val xcp = XContentType.JSON.xContent().createParser(NamedXContentRegistry.EMPTY, LoggingDeprecationHandler.INSTANCE, IndexManagementIndices.rollupTargetMappings)
//                ensureExpectedToken(XContentParser.Token.START_OBJECT, xcp.nextToken(), xcp::getTokenLocation) // start of mappings block
//                while (xcp.nextToken() != XContentParser.Token.END_OBJECT) {
//                    val fieldName = xcp.currentName()
//                    xcp.nextToken()
//
//                    when (fieldName) {
//                        "_meta" -> {
//                            logger.info("Field name is meta")
//                            ensureExpectedToken(XContentParser.Token.START_OBJECT, xcp.nextToken(), xcp::getTokenLocation) // start of mappings block
//                            while (xcp.nextToken() != XContentParser.Token.END_OBJECT) {
//                                val innerFieldName = xcp.currentName()
//                                xcp.nextToken()
//
//                                when (innerFieldName) {
//                                    "rollups" -> {
//                                        logger.info("Inner field name is rollups")
//                                    }
//                                }
//                            }
//                        }
//                    }
//                }


//                XContentFactory.jsonBuilder().startObject()
//                    .startObject("_meta")
//                    .startObject("rollups")
//                    .field(job.id, job, XCONTENT_WITHOUT_TYPE)
//                    .endObject().endObject().endObject()