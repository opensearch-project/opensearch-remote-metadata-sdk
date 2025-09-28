/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.remote.metadata.client;

import org.opensearch.action.get.GetResponse;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.XContentParser;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;

import static org.junit.Assert.assertNull;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class GetDataObjectResponseTests {

    private String testIndex;
    private String testId;
    private XContentParser testParser;
    private boolean testFailed;
    private Exception testCause;
    private RestStatus testStatus;
    private Map<String, Object> testSource;
    private GetResponse testGetResponse;

    @BeforeEach
    public void setUp() {
        testIndex = "test-index";
        testId = "test-id";
        testParser = mock(XContentParser.class);
        testFailed = true;
        testCause = mock(RuntimeException.class);
        testStatus = RestStatus.BAD_REQUEST;
        testSource = Map.of("foo", "bar");
        testGetResponse = mock(GetResponse.class);
        when(testGetResponse.getIndex()).thenReturn(testIndex);
        when(testGetResponse.getId()).thenReturn(testId);
        when(testGetResponse.getSourceAsMap()).thenReturn(testSource);
    }

    @Test
    public void testGetDataObjectResponse() {
        GetDataObjectResponse response = GetDataObjectResponse.builder()
            .index(testIndex)
            .id(testId)
            .parser(testParser)
            .failed(testFailed)
            .cause(testCause)
            .status(testStatus)
            .source(testSource)
            .build();

        assertEquals(testIndex, response.index());
        assertEquals(testId, response.id());
        assertSame(testParser, response.parser());
        assertEquals(testFailed, response.isFailed());
        assertSame(testCause, response.cause());
        assertEquals(testStatus, response.status());
        assertEquals(testSource, response.source());
    }

    @Test
    public void testGetDataObjectResponseWithGetResponse() throws IOException {
        GetDataObjectResponse response = new GetDataObjectResponse(testGetResponse);

        assertEquals(testIndex, response.index());
        assertEquals(testId, response.id());
        assertEquals(false, response.isFailed());
        assertNull(response.cause());
        assertNull(response.status());
        assertEquals(testSource, response.source());
        assertSame(testGetResponse, response.getResponse());
    }

    @Test
    public void testGetDataObjectResponseWithNullSource() {
        when(testGetResponse.getSourceAsMap()).thenReturn(null);
        GetDataObjectResponse response = new GetDataObjectResponse(testGetResponse);

        assertEquals(Collections.emptyMap(), response.source());
    }

    @Test
    public void test_parser_nullOrClosed() {
        GetDataObjectResponse response1 = new GetDataObjectResponse(testIndex, testId, null, testFailed, testCause, testStatus, testSource);
        XContentParser parser1 = response1.parser();
        Assertions.assertNull(parser1);

        when(testParser.isClosed()).thenReturn(true);
        GetDataObjectResponse response2 = new GetDataObjectResponse(
            testIndex,
            testId,
            testParser,
            testFailed,
            testCause,
            testStatus,
            testSource
        );
        XContentParser parser2 = response2.parser();
        Assertions.assertNull(parser2);
    }
}
