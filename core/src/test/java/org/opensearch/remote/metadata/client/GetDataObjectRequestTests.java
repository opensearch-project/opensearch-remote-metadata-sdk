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
    private String testCMKRole;
    private String testAssumeRole;
    private FetchSourceContext testFetchSourceContext;

    @BeforeEach
    public void setUp() {
        testIndex = "test-index";
        testId = "test-id";
        testTenantId = "test-tenant-id";
        testCMKRole = "test-cmk-role";
        testAssumeRole = "test-assume-role";
        testFetchSourceContext = mock(FetchSourceContext.class);
    }

    @Test
    public void testGetDataObjectRequest() {
        GetDataObjectRequest request = GetDataObjectRequest.builder()
            .index(testIndex)
            .id(testId)
            .tenantId(testTenantId)
            .fetchSourceContext(testFetchSourceContext)
            .cmkRoleArn(testCMKRole)
            .assumeRoleArn(testAssumeRole)
            .build();

        assertEquals(testIndex, request.index());
        assertEquals(testId, request.id());
        assertEquals(testTenantId, request.tenantId());
        assertEquals(testFetchSourceContext, request.fetchSourceContext());
        assertEquals(testCMKRole, request.cmkRoleArn());
        assertEquals(testAssumeRole, request.assumeRoleArn());
    }
}
