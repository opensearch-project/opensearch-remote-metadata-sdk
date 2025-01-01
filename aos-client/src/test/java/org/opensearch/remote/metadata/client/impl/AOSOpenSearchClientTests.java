package org.opensearch.remote.metadata.client.impl;

import org.opensearch.OpenSearchException;
import org.opensearch.client.opensearch.OpenSearchClient;
import org.opensearch.client.transport.aws.AwsSdk2Transport;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import static org.opensearch.remote.metadata.common.CommonValue.AWS_OPENSEARCH_SERVICE;
import static org.opensearch.remote.metadata.common.CommonValue.REMOTE_METADATA_ENDPOINT_KEY;
import static org.opensearch.remote.metadata.common.CommonValue.REMOTE_METADATA_REGION_KEY;
import static org.opensearch.remote.metadata.common.CommonValue.REMOTE_METADATA_SERVICE_NAME_KEY;
import static org.opensearch.remote.metadata.common.CommonValue.REMOTE_METADATA_TYPE_KEY;
import static org.opensearch.remote.metadata.common.CommonValue.REMOTE_OPENSEARCH;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class AOSOpenSearchClientTests {

    private AOSOpenSearchClient aosOpenSearchClient;

    @Mock
    private OpenSearchClient mockOpenSearchClient;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
        aosOpenSearchClient = new AOSOpenSearchClient();
    }

    @Test
    void testSupportsMetadataType() {
        assertTrue(aosOpenSearchClient.supportsMetadataType(AWS_OPENSEARCH_SERVICE));
        assertFalse(aosOpenSearchClient.supportsMetadataType(REMOTE_OPENSEARCH));
        assertFalse(aosOpenSearchClient.supportsMetadataType("unsupported_type"));
    }

    @Test
    void testInitialize() {
        Map<String, String> metadataSettings = new HashMap<>();
        metadataSettings.put(REMOTE_METADATA_TYPE_KEY, AWS_OPENSEARCH_SERVICE);
        metadataSettings.put(REMOTE_METADATA_ENDPOINT_KEY, "https://example.amazonaws.com");
        metadataSettings.put(REMOTE_METADATA_REGION_KEY, "us-west-2");
        metadataSettings.put(REMOTE_METADATA_SERVICE_NAME_KEY, "es");

        assertDoesNotThrow(() -> aosOpenSearchClient.initialize(metadataSettings));
    }

    @Test
    void testInitializeWithInvalidSettings() {
        Map<String, String> metadataSettings = new HashMap<>();
        metadataSettings.put(REMOTE_METADATA_TYPE_KEY, AWS_OPENSEARCH_SERVICE);
        // Missing endpoint and region

        assertThrows(OpenSearchException.class, () -> aosOpenSearchClient.initialize(metadataSettings));
    }

    @Test
    void testCreateOpenSearchClient() throws Exception {
        Map<String, String> metadataSettings = new HashMap<>();
        metadataSettings.put(REMOTE_METADATA_TYPE_KEY, AWS_OPENSEARCH_SERVICE);
        metadataSettings.put(REMOTE_METADATA_ENDPOINT_KEY, "https://example.amazonaws.com");
        metadataSettings.put(REMOTE_METADATA_REGION_KEY, "us-west-2");
        metadataSettings.put(REMOTE_METADATA_SERVICE_NAME_KEY, "es");

        aosOpenSearchClient.initialize(metadataSettings);

        OpenSearchClient client = aosOpenSearchClient.createOpenSearchClient();
        assertNotNull(client);
        assertTrue(client._transport() instanceof AwsSdk2Transport);
    }

    @Test
    void testClose() throws Exception {
        aosOpenSearchClient.openSearchClient = mockOpenSearchClient;
        when(mockOpenSearchClient._transport()).thenReturn(mock(AwsSdk2Transport.class));

        aosOpenSearchClient.close();

        verify(mockOpenSearchClient._transport(), times(1)).close();
    }
}
