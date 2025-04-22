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
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.XContentParser;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class DeleteDataObjectResponseTests {

    private String testIndex;
    private String testId;
    private XContentParser testParser;
    private boolean testFailed;
    private Exception testCause;
    private RestStatus testStatus;
    private DeleteResponse testDeleteResponse;
    private BulkItemResponse testBulkItemResponse;
    private DeleteResponse testBulkDeleteResponse;

    @BeforeEach
    public void setUp() {
        testIndex = "test-index";
        testId = "test-id";
        testParser = mock(XContentParser.class);
        testFailed = true;
        testCause = mock(RuntimeException.class);
        testStatus = RestStatus.BAD_REQUEST;
        testDeleteResponse = mock(DeleteResponse.class);
        when(testDeleteResponse.getIndex()).thenReturn(testIndex);
        when(testDeleteResponse.getId()).thenReturn(testId);

        testBulkDeleteResponse = mock(DeleteResponse.class);
        when(testBulkDeleteResponse.getIndex()).thenReturn(testIndex);
        when(testBulkDeleteResponse.getId()).thenReturn(testId);

        testBulkItemResponse = mock(BulkItemResponse.class);
        when(testBulkItemResponse.getIndex()).thenReturn(testIndex);
        when(testBulkItemResponse.getId()).thenReturn(testId);
        when(testBulkItemResponse.getResponse()).thenReturn(testBulkDeleteResponse);
    }

    @Test
    public void testDeleteDataObjectResponseBuilder() {
        DeleteDataObjectResponse response = DeleteDataObjectResponse.builder()
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
    public void testDeleteDataObjectResponseWithDeleteResponse() throws IOException {
        DeleteDataObjectResponse response = new DeleteDataObjectResponse(testDeleteResponse);

        assertEquals(testIndex, response.index());
        assertEquals(testId, response.id());
        assertEquals(false, response.isFailed());
        assertNull(response.cause());
        assertNull(response.status());
        assertSame(testDeleteResponse, response.deleteResponse());
    }

    @Test
    public void testParserCreationFromDeleteResponse() {
        DeleteDataObjectResponse response = new DeleteDataObjectResponse(testDeleteResponse);
        XContentParser parser = response.parser();
        assertNotNull(parser, "Parser should be created from DeleteResponse");
    }

    @Test
    public void testDeleteDataObjectResponse_FromBulkItemResponse() {
        when(testBulkItemResponse.isFailed()).thenReturn(false);
        DeleteDataObjectResponse response = new DeleteDataObjectResponse(testBulkItemResponse);

        assertEquals(testIndex, response.index());
        assertEquals(testId, response.id());
        assertEquals(false, response.isFailed());
        assertNull(response.cause());
        assertNull(response.status());
        assertSame(testBulkDeleteResponse, response.deleteResponse());
    }

    @Test
    public void testDeleteDataObjectResponse_FromFailedBulkItemResponse() {
        BulkItemResponse.Failure failure = mock(BulkItemResponse.Failure.class);
        when(failure.getCause()).thenReturn(testCause);
        when(failure.getStatus()).thenReturn(testStatus);

        when(testBulkItemResponse.isFailed()).thenReturn(true);
        when(testBulkItemResponse.getFailure()).thenReturn(failure);
        when(testBulkItemResponse.getResponse()).thenReturn(null);

        DeleteDataObjectResponse response = new DeleteDataObjectResponse(testBulkItemResponse);

        assertEquals(testIndex, response.index());
        assertEquals(testId, response.id());
        assertTrue(response.isFailed());
        assertSame(testCause, response.cause());
        assertEquals(testStatus, response.status());
        assertNull(response.deleteResponse());
    }

    @Test
    public void testDeleteDataObjectResponse_FromBulkItemResponseWrongType() {
        IndexResponse wrongResponse = mock(IndexResponse.class);
        when(testBulkItemResponse.getResponse()).thenReturn(wrongResponse);

        assertThrows(OpenSearchException.class, () -> new DeleteDataObjectResponse(testBulkItemResponse));
    }
}
