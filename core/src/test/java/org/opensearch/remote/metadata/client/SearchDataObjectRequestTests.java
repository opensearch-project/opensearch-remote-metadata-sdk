/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.remote.metadata.client;

import org.opensearch.search.builder.SearchSourceBuilder;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

public class SearchDataObjectRequestTests {

    private String[] testIndices;
    private String testTenantId;
    private SearchSourceBuilder testSearchSourceBuilder;

    @BeforeEach
    public void setUp() {
        testIndices = new String[] { "test-index" };
        testTenantId = "test-tenant-id";
        testSearchSourceBuilder = new SearchSourceBuilder();
    }

    @Test
    public void testSearchDataObjectRequest() {
        String testRouting = "test-routing";
        SearchDataObjectRequest request = SearchDataObjectRequest.builder()
            .indices(testIndices)
            .tenantId(testTenantId)
            .searchSourceBuilder(testSearchSourceBuilder)
            .routing(testRouting)
            .build();

        assertArrayEquals(testIndices, request.indices());
        assertEquals(testTenantId, request.tenantId());
        assertEquals(testSearchSourceBuilder, request.searchSourceBuilder());
        assertEquals(testRouting, request.routing());

        // Test null routing
        SearchDataObjectRequest requestNoRouting = SearchDataObjectRequest.builder()
            .indices(testIndices)
            .tenantId(testTenantId)
            .searchSourceBuilder(testSearchSourceBuilder)
            .build();
        assertNull(requestNoRouting.routing());
    }
}
