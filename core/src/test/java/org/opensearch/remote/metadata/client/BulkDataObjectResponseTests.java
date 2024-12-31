/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.remote.metadata.client;

import org.opensearch.core.xcontent.XContentParser;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;

import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class BulkDataObjectResponseTests {
    @Mock
    XContentParser parser;

    @BeforeEach
    public void setup() throws IOException {
        MockitoAnnotations.openMocks(this);
    }

    @Test
    public void testBulkDataObjectResponse() {
        DataObjectResponse[] responses = List.of(
            PutDataObjectResponse.builder().build(),
            UpdateDataObjectResponse.builder().build(),
            DeleteDataObjectResponse.builder().build()
        ).toArray(new DataObjectResponse[0]);

        BulkDataObjectResponse response = new BulkDataObjectResponse(responses, 1L, false, parser);

        assertEquals(3, response.getResponses().length);
        assertEquals(1L, response.getTookInMillis());
        assertEquals(-1L, response.getIngestTookInMillis());
        assertFalse(response.hasFailures());
        assertSame(parser, response.parser());
    }

    @Test
    public void testBulkDataObjectRequest_Failures() {
        DataObjectResponse[] responses = List.of(
            PutDataObjectResponse.builder().build(),
            DeleteDataObjectResponse.builder().failed(true).build()
        ).toArray(new DataObjectResponse[0]);

        BulkDataObjectResponse response = new BulkDataObjectResponse(responses, 1L, true, parser);

        assertEquals(2, response.getResponses().length);
        assertEquals(1L, response.getTookInMillis());
        assertEquals(-1L, response.getIngestTookInMillis());
        assertTrue(response.hasFailures());
        assertSame(parser, response.parser());
    }
}
