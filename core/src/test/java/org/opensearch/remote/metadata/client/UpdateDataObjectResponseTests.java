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
import org.opensearch.action.update.UpdateResponse;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.XContentParser;
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

public class UpdateDataObjectResponseTests {

    private String testIndex;
    private String testId;
    private XContentParser testParser;
    private boolean testFailed;
    private Exception testCause;
    private RestStatus testStatus;
    private UpdateResponse testUpdateResponse;
    private BulkItemResponse testBulkItemResponse;
    private UpdateResponse testBulkUpdateResponse;

    @BeforeEach
    public void setUp() {
        testIndex = "test-index";
        testId = "test-id";
        testParser = mock(XContentParser.class);
        testFailed = true;
        testCause = mock(RuntimeException.class);
        testStatus = RestStatus.BAD_REQUEST;
        testUpdateResponse = mock(UpdateResponse.class);
        when(testUpdateResponse.getIndex()).thenReturn(testIndex);
        when(testUpdateResponse.getId()).thenReturn(testId);

        testBulkUpdateResponse = mock(UpdateResponse.class);
        when(testBulkUpdateResponse.getIndex()).thenReturn(testIndex);
        when(testBulkUpdateResponse.getId()).thenReturn(testId);

        testBulkItemResponse = mock(BulkItemResponse.class);
        when(testBulkItemResponse.getIndex()).thenReturn(testIndex);
        when(testBulkItemResponse.getId()).thenReturn(testId);
        when(testBulkItemResponse.getResponse()).thenReturn(testBulkUpdateResponse);
    }

    @Test
    public void testUpdateDataObjectResponseBuilder() {
        UpdateDataObjectResponse response = UpdateDataObjectResponse.builder()
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
    public void testUpdateDataObjectResponseWithUpdateResponse() throws IOException {
        UpdateDataObjectResponse response = new UpdateDataObjectResponse(testUpdateResponse);

        assertEquals(testIndex, response.index());
        assertEquals(testId, response.id());
        assertNotNull(response.parser()); // Parser is created from UpdateResponse
        assertEquals(false, response.isFailed());
        assertNull(response.cause());
        assertNull(response.status());
        assertSame(testUpdateResponse, response.updateResponse());
    }

    @Test
    public void testParserCreationFromUpdateResponse() {
        UpdateDataObjectResponse response = new UpdateDataObjectResponse(testUpdateResponse);
        XContentParser parser = response.parser();
        assertNotNull(parser);
    }

    @Test
    public void testUpdateDataObjectResponse_FromBulkItemResponse() {
        when(testBulkItemResponse.isFailed()).thenReturn(false);
        UpdateDataObjectResponse response = new UpdateDataObjectResponse(testBulkItemResponse);

        assertEquals(testIndex, response.index());
        assertEquals(testId, response.id());
        assertEquals(false, response.isFailed());
        assertNull(response.cause());
        assertNull(response.status());
        assertSame(testBulkUpdateResponse, response.updateResponse());
    }

    @Test
    public void testUpdateDataObjectResponse_FromFailedBulkItemResponse() {
        BulkItemResponse.Failure failure = mock(BulkItemResponse.Failure.class);
        when(failure.getCause()).thenReturn(testCause);
        when(failure.getStatus()).thenReturn(testStatus);

        when(testBulkItemResponse.isFailed()).thenReturn(true);
        when(testBulkItemResponse.getFailure()).thenReturn(failure);
        when(testBulkItemResponse.getResponse()).thenReturn(null);

        UpdateDataObjectResponse response = new UpdateDataObjectResponse(testBulkItemResponse);

        assertEquals(testIndex, response.index());
        assertEquals(testId, response.id());
        assertTrue(response.isFailed());
        assertSame(testCause, response.cause());
        assertEquals(testStatus, response.status());
        assertNull(response.updateResponse());
    }

    @Test
    public void testUpdateDataObjectResponse_FromBulkItemResponseWrongType() {
        DeleteResponse wrongResponse = mock(DeleteResponse.class);
        when(testBulkItemResponse.getResponse()).thenReturn(wrongResponse);

        assertThrows(OpenSearchException.class, () -> new UpdateDataObjectResponse(testBulkItemResponse));
    }
}
