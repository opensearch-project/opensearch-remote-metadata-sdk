/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.remote.metadata.client;

import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.XContentParser;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.Mockito.mock;

public class GetDataObjectResponseTests {

    private String testIndex;
    private String testId;
    private XContentParser testParser;
    private boolean testFailed;
    private Exception testCause;
    private RestStatus testStatus;
    private Map<String, Object> testSource;

    @BeforeEach
    public void setUp() {
        testIndex = "test-index";
        testId = "test-id";
        testParser = mock(XContentParser.class);
        testFailed = true;
        testCause = mock(RuntimeException.class);
        testStatus = RestStatus.BAD_REQUEST;
        testSource = Map.of("foo", "bar");
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
}
