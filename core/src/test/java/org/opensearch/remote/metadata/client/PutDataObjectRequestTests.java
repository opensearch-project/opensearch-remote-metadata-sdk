/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.remote.metadata.client;

import org.opensearch.action.support.WriteRequest.RefreshPolicy;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.xcontent.MediaType;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.remote.metadata.client.PutDataObjectRequest.Builder;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

public class PutDataObjectRequestTests {

    private String testIndex;
    private String testId;
    private String testTenantId;
    private ToXContentObject testDataObject;
    private RefreshPolicy testRefreshPolicy;
    private Long testIfSeqNo;
    private Long testIfPrimaryTerm;
    private TimeValue testTimeout;

    @BeforeEach
    public void setUp() {
        testIndex = "test-index";
        testId = "test-id";
        testTenantId = "test-tenant-id";
        testDataObject = mock(ToXContentObject.class);
        testRefreshPolicy = RefreshPolicy.NONE;
        testIfSeqNo = 1234L;
        testIfPrimaryTerm = 5678L;
        testTimeout = TimeValue.timeValueSeconds(30);
    }

    @Test
    public void testPutDataObjectRequest() {
        Builder builder = PutDataObjectRequest.builder().index(testIndex).id(testId).tenantId(testTenantId).dataObject(testDataObject);
        PutDataObjectRequest request = builder.build();

        assertEquals(testIndex, request.index());
        assertEquals(testId, request.id());
        assertEquals(testTenantId, request.tenantId());
        assertTrue(request.overwriteIfExists());
        assertSame(testDataObject, request.dataObject());

        // Verify Default values
        assertEquals(RefreshPolicy.IMMEDIATE, request.refreshPolicy());
        assertNull(request.ifSeqNo());
        assertNull(request.ifPrimaryTerm());
        assertNull(request.timeout());

        // Test with custom values for optional fields
        builder.overwriteIfExists(false)
                .refreshPolicy(testRefreshPolicy)
                .ifSeqNo(testIfSeqNo)
                .ifPrimaryTerm(testIfPrimaryTerm)
                .timeout(testTimeout);
        request = builder.build();
        assertFalse(request.overwriteIfExists());
        assertEquals(testRefreshPolicy, request.refreshPolicy());
        assertEquals(testIfSeqNo, request.ifSeqNo());
        assertEquals(testIfPrimaryTerm, request.ifPrimaryTerm());
        assertEquals(testTimeout, request.timeout());
    }

    @Test
    public void testPutDataObjectRequestWithMap() throws IOException {
        Map<String, Object> dataObjectMap = Map.of("key1", "value1", "key2", "value2");

        Builder builder = PutDataObjectRequest.builder()
                .index(testIndex)
                .id(testId)
                .tenantId(testTenantId)
                .dataObject(dataObjectMap)
                .refreshPolicy(testRefreshPolicy)
                .ifSeqNo(testIfSeqNo)
                .ifPrimaryTerm(testIfPrimaryTerm)
                .timeout(testTimeout);
        PutDataObjectRequest request = builder.build();

        // Verify the index, id, tenantId, and overwriteIfExists fields
        assertEquals(testIndex, request.index());
        assertEquals(testId, request.id());
        assertEquals(testTenantId, request.tenantId());
        assertTrue(request.overwriteIfExists());
        // Verify additional fields
        assertEquals(testRefreshPolicy, request.refreshPolicy());
        assertEquals(testIfSeqNo, request.ifSeqNo());
        assertEquals(testIfPrimaryTerm, request.ifPrimaryTerm());
        assertEquals(testTimeout, request.timeout());

        // Verify the dataObject field by converting it back to a Map and comparing
        ToXContentObject dataObject = request.dataObject();
        XContentBuilder contentBuilder = XContentFactory.jsonBuilder();
        dataObject.toXContent(contentBuilder, ToXContent.EMPTY_PARAMS);
        contentBuilder.flush();

        BytesReference bytes = BytesReference.bytes(contentBuilder);
        Map<String, Object> resultingMap = XContentHelper.convertToMap(bytes, false, (MediaType) XContentType.JSON).v2();

        assertEquals(dataObjectMap, resultingMap);
    }

    @Test
    public void testPutDataObjectRequestValidation() {
        Builder builder = PutDataObjectRequest.builder()
                .index(testIndex)
                .id(testId)
                .tenantId(testTenantId)
                .dataObject(testDataObject);

        // Test invalid sequence number
        try {
            builder.ifSeqNo(-1L);
            builder.build();
            assertTrue(false, "Expected IllegalArgumentException for negative sequence number");
        } catch (IllegalArgumentException e) {
            assertEquals("sequence numbers must be non negative. got [-1].", e.getMessage());
        }

        // Test invalid primary term
        try {
            builder.ifPrimaryTerm(-1L);
            builder.build();
            assertTrue(false, "Expected IllegalArgumentException for negative primary term");
        } catch (IllegalArgumentException e) {
            assertEquals("primary term must be non negative. got [-1]", e.getMessage());
        }

        // Test valid unassigned sequence number
        builder.ifSeqNo(org.opensearch.index.seqno.SequenceNumbers.UNASSIGNED_SEQ_NO);
        PutDataObjectRequest request = builder.build();
        assertEquals(org.opensearch.index.seqno.SequenceNumbers.UNASSIGNED_SEQ_NO, request.ifSeqNo());

        // Test and validate that this is a write request
        assertTrue(builder.build().isWriteRequest());
    }
}