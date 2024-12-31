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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;

public class SearchDataObjectResponseTests {

    private XContentParser testParser;

    @BeforeEach
    public void setUp() {
        testParser = mock(XContentParser.class);
    }

    @Test
    public void testSearchDataObjectResponse() {
        SearchDataObjectResponse response = SearchDataObjectResponse.builder().parser(testParser).build();

        assertEquals(testParser, response.parser());
    }
}
