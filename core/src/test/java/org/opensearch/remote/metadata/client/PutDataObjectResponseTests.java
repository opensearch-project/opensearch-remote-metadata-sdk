/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.remote.metadata.client;

import org.opensearch.OpenSearchException;
import org.opensearch.action.bulk.BulkItemResponse;
import org.opensearch.action.delete.DeleteResponse;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.remote.metadata.common.SdkClientUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class PutDataObjectResponseTests {

    private String testIndex;
    private String testId;
    private XContentParser testParser;
    private boolean testFailed;
    private Exception testCause;
    private RestStatus testStatus;
    private IndexResponse testIndexResponse;
    private BulkItemResponse testBulkItemResponse;
    private IndexResponse testBulkIndexResponse;

    @BeforeEach
    public void setUp() {
        testIndex = "test-index";
        testId = "test-id";
        testParser = mock(XContentParser.class);
        testFailed = true;
        testCause = mock(RuntimeException.class);
        testStatus = RestStatus.BAD_REQUEST;
        testIndexResponse = new IndexResponse(new ShardId(testIndex, "_na_", 0), testId, 1, 1, 1, true);

        testBulkIndexResponse = mock(IndexResponse.class);
        when(testBulkIndexResponse.getIndex()).thenReturn(testIndex);
        when(testBulkIndexResponse.getId()).thenReturn(testId);

        testBulkItemResponse = mock(BulkItemResponse.class);
        when(testBulkItemResponse.getIndex()).thenReturn(testIndex);
        when(testBulkItemResponse.getId()).thenReturn(testId);
        when(testBulkItemResponse.getResponse()).thenReturn(testBulkIndexResponse);

    }

    @Test
    public void testPutDataObjectResponse_FromBuilder() {
        PutDataObjectResponse response = PutDataObjectResponse.builder()
            .index(testIndex)
            .id(testId)
            .parser(testParser)
            .failed(testFailed)
            .cause(testCause)
            .status(testStatus)
            .build();

        assertEquals(testIndex, response.index());
        assertEquals(testId, response.id());
        assertSame(testParser, response.parser());
        assertEquals(testFailed, response.isFailed());
        assertSame(testCause, response.cause());
        assertEquals(testStatus, response.status());
    }

    @Test
    public void testPutDataObjectResponse_FromIndexResponse() {
        PutDataObjectResponse response = new PutDataObjectResponse(testIndexResponse);

        assertEquals(testIndex, response.index());
        assertEquals(testId, response.id());
        assertNotNull(response.parser());
    }

    @Test
    public void testPutDataObjectResponse_IndexResponseGetter() throws IOException {
        PutDataObjectResponse response = new PutDataObjectResponse(testIndexResponse);
        IndexResponse retrievedResponse = response.indexResponse();

        assertSame(testIndexResponse, retrievedResponse);
    }

    @Test
    public void testPutDataObjectResponse_IndexResponseFromParser() throws IOException {
        // Create a minimal valid index response JSON
        String indexResponseJson = "{\n"
            + "  \"_index\": \""
            + testIndex
            + "\",\n"
            + "  \"_id\": \""
            + testId
            + "\",\n"
            + "  \"_version\": 1,\n"
            + "  \"result\": \"created\",\n"
            + "  \"_shards\": {\n"
            + "    \"total\": 2,\n"
            + "    \"successful\": 1,\n"
            + "    \"failed\": 0\n"
            + "  },\n"
            + "  \"_seq_no\": 0,\n"
            + "  \"_primary_term\": 1\n"
            + "}";

        XContentParser parser = SdkClientUtils.createParser(indexResponseJson);
        PutDataObjectResponse response = new PutDataObjectResponse(testIndex, testId, parser, false, null, RestStatus.CREATED);

        IndexResponse parsedResponse = response.indexResponse();
        assertNotNull(parsedResponse);
        assertEquals(testIndex, parsedResponse.getIndex());
        assertEquals(testId, parsedResponse.getId());
    }

    @Test
    public void testPutDataObjectResponse_FromBulkItemResponse() {
        when(testBulkItemResponse.isFailed()).thenReturn(false);
        PutDataObjectResponse response = new PutDataObjectResponse(testBulkItemResponse);

        assertEquals(testIndex, response.index());
        assertEquals(testId, response.id());
        assertEquals(false, response.isFailed());
        assertNull(response.cause());
        assertNull(response.status());
        assertSame(testBulkIndexResponse, response.indexResponse());
    }

    @Test
    public void testPutDataObjectResponse_FromFailedBulkItemResponse() {
        BulkItemResponse.Failure failure = mock(BulkItemResponse.Failure.class);
        when(failure.getCause()).thenReturn(testCause);
        when(failure.getStatus()).thenReturn(testStatus);

        when(testBulkItemResponse.isFailed()).thenReturn(true);
        when(testBulkItemResponse.getFailure()).thenReturn(failure);
        when(testBulkItemResponse.getResponse()).thenReturn(null);

        PutDataObjectResponse response = new PutDataObjectResponse(testBulkItemResponse);

        assertEquals(testIndex, response.index());
        assertEquals(testId, response.id());
        assertTrue(response.isFailed());
        assertSame(testCause, response.cause());
        assertEquals(testStatus, response.status());
        assertNull(response.indexResponse());
    }

    @Test
    public void testPutDataObjectResponse_FromBulkItemResponseWrongType() {
        DeleteResponse wrongResponse = mock(DeleteResponse.class);
        when(testBulkItemResponse.getResponse()).thenReturn(wrongResponse);

        assertThrows(OpenSearchException.class, () -> new PutDataObjectResponse(testBulkItemResponse));
    }
}
