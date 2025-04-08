/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.remote.metadata.client;

import org.opensearch.action.DocWriteRequest;
import org.opensearch.action.DocWriteResponse.Result;
import org.opensearch.action.bulk.BulkItemResponse;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.action.delete.DeleteResponse;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.action.update.UpdateResponse;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.remote.metadata.common.SdkClientUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;

import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import static org.opensearch.action.bulk.BulkResponse.NO_INGEST_TOOK;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;

public class BulkDataObjectResponseTests {
    @Mock
    BulkResponse testBulkResponse;

    private long testTookInMillis;
    private boolean testHasFailures;
    private XContentParser testParser;
    private BulkItemResponse[] testBulkItems;

    @BeforeEach
    public void setup() throws IOException {
        MockitoAnnotations.openMocks(this);

        testTookInMillis = 100L;
        testHasFailures = false;

        ShardId shardId = new ShardId("test-index", "_na_", 1);
        testBulkItems = new BulkItemResponse[] {
            new BulkItemResponse(0, DocWriteRequest.OpType.INDEX, new IndexResponse(shardId, "1", 1, 1, 1, true)),
            new BulkItemResponse(1, DocWriteRequest.OpType.UPDATE, new UpdateResponse(shardId, "2", 1, 1, 1, Result.UPDATED)),
            new BulkItemResponse(2, DocWriteRequest.OpType.DELETE, new DeleteResponse(shardId, "3", 1, 1, 1, true)) };

        // Setup BulkResponse mock
        when(testBulkResponse.getTook()).thenReturn(TimeValue.timeValueMillis(testTookInMillis));
        when(testBulkResponse.hasFailures()).thenReturn(testHasFailures);
        when(testBulkResponse.getItems()).thenReturn(testBulkItems);
        when(testBulkResponse.getIngestTookInMillis()).thenReturn(NO_INGEST_TOOK);

        // Create a real parser from a minimal valid bulk response JSON
        String bulkResponseJson = "{\"took\": 100, \"errors\": false, \"items\": []}";
        testParser = SdkClientUtils.createParser(bulkResponseJson);
    }

    @Test
    public void testBulkDataObjectResponse() {
        DataObjectResponse[] responses = List.of(
            PutDataObjectResponse.builder().build(),
            UpdateDataObjectResponse.builder().build(),
            DeleteDataObjectResponse.builder().build()
        ).toArray(new DataObjectResponse[0]);

        BulkDataObjectResponse response = new BulkDataObjectResponse(responses, 1L, false, testParser);

        assertEquals(3, response.getResponses().length);
        assertEquals(1L, response.getTookInMillis());
        assertEquals(-1L, response.getIngestTookInMillis());
        assertFalse(response.hasFailures());
        assertSame(testParser, response.parser());
    }

    @Test
    public void testBulkDataObjectRequest_Failures() {
        DataObjectResponse[] responses = List.of(
            PutDataObjectResponse.builder().build(),
            DeleteDataObjectResponse.builder().failed(true).build()
        ).toArray(new DataObjectResponse[0]);

        BulkDataObjectResponse response = new BulkDataObjectResponse(responses, 1L, true, testParser);

        assertEquals(2, response.getResponses().length);
        assertEquals(1L, response.getTookInMillis());
        assertEquals(-1L, response.getIngestTookInMillis());
        assertTrue(response.hasFailures());
        assertSame(testParser, response.parser());
    }

    @Test
    public void testBulkResponse_FromParser() throws IOException {
        // Create response with parser
        DataObjectResponse[] responses = new DataObjectResponse[0];
        BulkDataObjectResponse response = new BulkDataObjectResponse(responses, 1L, false, testParser);

        BulkResponse parsedResponse = response.bulkResponse();
        assertNotNull(parsedResponse);
        assertEquals(100, parsedResponse.getTook().millis());
        assertFalse(parsedResponse.hasFailures());
    }

    @Test
    public void testParser_FromBulkResponse() throws IOException {
        // Setup mock BulkResponse with proper XContent behavior
        BulkDataObjectResponse response = new BulkDataObjectResponse(testBulkResponse);

        // Test that we can get a parser
        XContentParser parser = response.parser();
        assertNotNull(parser, "Parser should be created from BulkResponse");

        // Test that we can get the bulk response back
        BulkResponse retrievedResponse = response.bulkResponse();
        assertNotNull(retrievedResponse);
        assertEquals(testTookInMillis, retrievedResponse.getTook().millis());
        assertEquals(testHasFailures, retrievedResponse.hasFailures());
    }

    @Test
    public void testParser_NullHandling() {
        // Test when both parser and bulkResponse are null
        BulkDataObjectResponse response = new BulkDataObjectResponse(new DataObjectResponse[0], 1L, false, null);
        assertNull(response.parser());
    }

    @Test
    public void testBulkResponse_NullHandling() {
        // Test when both parser and bulkResponse are null
        BulkDataObjectResponse response = new BulkDataObjectResponse(new DataObjectResponse[0], 1L, false, null);
        assertNull(response.bulkResponse());
    }
}
