/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.remote.metadata.client;

import org.opensearch.search.fetch.subphase.FetchSourceContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;

public class GetDataObjectRequestTests {

    private String testIndex;
    private String testId;
    private String testTenantId;
    private FetchSourceContext testFetchSourceContext;

    @BeforeEach
    public void setUp() {
        testIndex = "test-index";
        testId = "test-id";
        testTenantId = "test-tenant-id";
        testFetchSourceContext = mock(FetchSourceContext.class);
    }

    @Test
    public void testGetDataObjectRequest() {
        GetDataObjectRequest request = GetDataObjectRequest.builder()
            .index(testIndex)
            .id(testId)
            .tenantId(testTenantId)
            .fetchSourceContext(testFetchSourceContext)
            .build();

        assertEquals(testIndex, request.index());
        assertEquals(testId, request.id());
        assertEquals(testTenantId, request.tenantId());
        assertEquals(testFetchSourceContext, request.fetchSourceContext());
    }

    @Test
    public void testGetDataObjectRequestBuilderWithExistingRequest() {
        GetDataObjectRequest originalRequest = GetDataObjectRequest.builder()
            .index(testIndex)
            .id(testId)
            .tenantId(testTenantId)
            .fetchSourceContext(testFetchSourceContext)
            .build();

        GetDataObjectRequest copiedRequest = GetDataObjectRequest.builder(originalRequest).tenantId("new-tenant-id").build();

        assertEquals(testIndex, copiedRequest.index());
        assertEquals(testId, copiedRequest.id());
        assertEquals("new-tenant-id", copiedRequest.tenantId());
        assertEquals(testFetchSourceContext, copiedRequest.fetchSourceContext());
    }
}
