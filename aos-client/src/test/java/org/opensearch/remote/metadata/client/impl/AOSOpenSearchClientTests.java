package org.opensearch.remote.metadata.client.impl;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;

import software.amazon.awssdk.utils.ImmutableMap;

import org.opensearch.OpenSearchException;
import org.opensearch.client.json.jackson.JacksonJsonpMapper;
import org.opensearch.client.opensearch.OpenSearchAsyncClient;
import org.opensearch.client.opensearch.core.GetRequest;
import org.opensearch.client.opensearch.core.GetResponse;
import org.opensearch.client.transport.OpenSearchTransport;
import org.opensearch.client.transport.aws.AwsSdk2Transport;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.concurrent.OpenSearchExecutors;
import org.opensearch.remote.metadata.client.GetDataObjectRequest;
import org.opensearch.remote.metadata.client.GetDataObjectResponse;
import org.opensearch.threadpool.ScalingExecutorBuilder;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import static org.opensearch.remote.metadata.common.CommonValue.AWS_OPENSEARCH_SERVICE;
import static org.opensearch.remote.metadata.common.CommonValue.REMOTE_METADATA_ENDPOINT_KEY;
import static org.opensearch.remote.metadata.common.CommonValue.REMOTE_METADATA_REGION_KEY;
import static org.opensearch.remote.metadata.common.CommonValue.REMOTE_METADATA_SERVICE_NAME_KEY;
import static org.opensearch.remote.metadata.common.CommonValue.REMOTE_METADATA_TYPE_KEY;
import static org.opensearch.remote.metadata.common.CommonValue.REMOTE_OPENSEARCH;
import static org.opensearch.remote.metadata.common.CommonValue.TENANT_ID_FIELD_KEY;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class AOSOpenSearchClientTests {

    private AOSOpenSearchClient aosOpenSearchClient;
    private static final String TEST_THREAD_POOL = "test_pool";
    private static final String TEST_INDEX = "test_index";
    private static final String TEST_ID = "test_id";
    private static final String TEST_TENANT_ID = "test_tenant";
    private static final String TEST_GLOBAL_TENANT_ID = "test_global_tenant";
    private static final TimeValue TEST_GLOBAL_RESOURCE_CACHE_TTL = TimeValue.timeValueMillis(5 * 60 * 1000);

    @Mock
    private OpenSearchAsyncClient mockOpenSearchAsyncClient;

    @Mock
    private OpenSearchTransport transport;

    private static TestThreadPool testThreadPool = new TestThreadPool(
        AOSOpenSearchClientTests.class.getName(),
        new ScalingExecutorBuilder(
            TEST_THREAD_POOL,
            1,
            Math.max(1, OpenSearchExecutors.allocatedProcessors(Settings.EMPTY) - 1),
            TimeValue.timeValueMinutes(1),
            TEST_THREAD_POOL
        )
    );

    @AfterAll
    static void cleanup() {
        ThreadPool.terminate(testThreadPool, 500, TimeUnit.MILLISECONDS);
    }

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
        aosOpenSearchClient = new AOSOpenSearchClient();
        when(mockOpenSearchAsyncClient._transport()).thenReturn(transport);
        when(transport.jsonpMapper()).thenReturn(
            new JacksonJsonpMapper(
                new ObjectMapper().setPropertyNamingStrategy(PropertyNamingStrategies.SNAKE_CASE)
                    .setSerializationInclusion(JsonInclude.Include.NON_NULL)
            )
        );
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

        OpenSearchAsyncClient client = aosOpenSearchClient.createOpenSearchAsyncClient();
        assertNotNull(client);
        assertTrue(client._transport() instanceof AwsSdk2Transport);
    }

    @Test
    void testClose() throws Exception {
        aosOpenSearchClient.openSearchAsyncClient = mockOpenSearchAsyncClient;
        when(mockOpenSearchAsyncClient._transport()).thenReturn(mock(AwsSdk2Transport.class));

        aosOpenSearchClient.close();

        verify(mockOpenSearchAsyncClient._transport(), times(1)).close();
    }

    @Test
    void testGetDataObjectWithUserTenantId() throws IOException {
        testInitialize();
        aosOpenSearchClient.openSearchAsyncClient = mockOpenSearchAsyncClient;
        GetDataObjectRequest request = mock(GetDataObjectRequest.class);
        when(request.index()).thenReturn(TEST_INDEX);
        when(request.id()).thenReturn(TEST_ID);
        when(request.tenantId()).thenReturn(TEST_TENANT_ID);

        GetResponse<Map<String, Object>> getResponse = new GetResponse.Builder<Map<String, Object>>().index(TEST_INDEX)
            .id("mockId")
            .found(true)
            .source(ImmutableMap.of(TENANT_ID_FIELD_KEY, TEST_TENANT_ID))
            .build();
        when(mockOpenSearchAsyncClient.get(any(GetRequest.class), any(Class.class))).thenReturn(
            CompletableFuture.completedFuture(getResponse)
        );

        GetDataObjectResponse result = aosOpenSearchClient.getDataObjectAsync(request, testThreadPool.executor(TEST_THREAD_POOL), true)
            .toCompletableFuture()
            .join();

        assertNotNull(result);
        assertEquals(TEST_TENANT_ID, result.source().get(TENANT_ID_FIELD_KEY));
    }

    @Test
    void test_getDataObject_withGlobalTenantId_resourceFound() throws IOException {
        testInitialize();
        aosOpenSearchClient.openSearchAsyncClient = mockOpenSearchAsyncClient;
        aosOpenSearchClient.setGlobalTenantId(TEST_GLOBAL_TENANT_ID);
        aosOpenSearchClient.setGlobalResourceCacheTTL(TEST_GLOBAL_RESOURCE_CACHE_TTL);
        GetDataObjectRequest request = mock(GetDataObjectRequest.class);
        when(request.index()).thenReturn(TEST_INDEX);
        when(request.id()).thenReturn(TEST_ID);
        when(request.tenantId()).thenReturn(TEST_TENANT_ID);
        Map<String, Object> source = new HashMap<>();
        source.put(TENANT_ID_FIELD_KEY, TEST_GLOBAL_TENANT_ID);
        GetResponse<Map<String, Object>> getResponse = new GetResponse.Builder<Map<String, Object>>().index(TEST_INDEX)
            .id("mockId")
            .found(true)
            .source(source)
            .build();
        when(mockOpenSearchAsyncClient.get(any(GetRequest.class), any(Class.class))).thenReturn(
            CompletableFuture.completedFuture(getResponse)
        );

        GetDataObjectResponse result = aosOpenSearchClient.getDataObjectAsync(request, testThreadPool.executor(TEST_THREAD_POOL), true)
            .toCompletableFuture()
            .join();
        assertEquals(TEST_TENANT_ID, result.source().get(TENANT_ID_FIELD_KEY));
        assertEquals(TEST_TENANT_ID, result.getResponse().getSourceAsMap().get(TENANT_ID_FIELD_KEY));
    }

    @Test
    void test_getDataObject_globalResourceDisabled_resourceFound() throws IOException {
        testInitialize();
        aosOpenSearchClient.openSearchAsyncClient = mockOpenSearchAsyncClient;
        GetDataObjectRequest request = mock(GetDataObjectRequest.class);
        when(request.index()).thenReturn(TEST_INDEX);
        when(request.id()).thenReturn(TEST_ID);
        when(request.tenantId()).thenReturn(TEST_TENANT_ID);
        Map<String, Object> source = new HashMap<>();
        source.put(TENANT_ID_FIELD_KEY, TEST_TENANT_ID);
        GetResponse<Map<String, Object>> getResponse = new GetResponse.Builder<Map<String, Object>>().index(TEST_INDEX)
            .id("mockId")
            .found(true)
            .source(source)
            .build();
        when(mockOpenSearchAsyncClient.get(any(GetRequest.class), any(Class.class))).thenReturn(
            CompletableFuture.completedFuture(getResponse)
        );

        GetDataObjectResponse result = aosOpenSearchClient.getDataObjectAsync(request, testThreadPool.executor(TEST_THREAD_POOL), true)
            .toCompletableFuture()
            .join();
        assertEquals(TEST_TENANT_ID, result.source().get(TENANT_ID_FIELD_KEY));
    }

    @Test
    void test_getDataObject_withGlobalTenantId_resourceFound_readAgainFromCache() throws IOException {
        testInitialize();
        aosOpenSearchClient.openSearchAsyncClient = mockOpenSearchAsyncClient;
        aosOpenSearchClient.setGlobalTenantId(TEST_GLOBAL_TENANT_ID);
        aosOpenSearchClient.setGlobalResourceCacheTTL(TEST_GLOBAL_RESOURCE_CACHE_TTL);
        GetDataObjectRequest request = mock(GetDataObjectRequest.class);
        when(request.index()).thenReturn(TEST_INDEX);
        when(request.id()).thenReturn(TEST_ID);
        when(request.tenantId()).thenReturn(TEST_TENANT_ID);
        Map<String, Object> source = new HashMap<>();
        source.put(TENANT_ID_FIELD_KEY, TEST_GLOBAL_TENANT_ID);
        GetResponse<Map<String, Object>> getResponse = new GetResponse.Builder<Map<String, Object>>().index(TEST_INDEX)
            .id("mockId")
            .found(true)
            .source(source)
            .build();
        when(mockOpenSearchAsyncClient.get(any(GetRequest.class), any(Class.class))).thenReturn(
            CompletableFuture.completedFuture(getResponse)
        );

        GetDataObjectResponse result = aosOpenSearchClient.getDataObjectAsync(request, testThreadPool.executor(TEST_THREAD_POOL), true)
            .toCompletableFuture()
            .join();
        assertEquals(TEST_TENANT_ID, result.source().get(TENANT_ID_FIELD_KEY));
        assertEquals(TEST_TENANT_ID, result.getResponse().getSourceAsMap().get(TENANT_ID_FIELD_KEY));

        GetDataObjectResponse resultFromCache = aosOpenSearchClient.getDataObjectAsync(
            request,
            testThreadPool.executor(TEST_THREAD_POOL),
            true
        ).toCompletableFuture().join();
        assertEquals(TEST_TENANT_ID, resultFromCache.source().get(TENANT_ID_FIELD_KEY));
        assertEquals(TEST_TENANT_ID, result.getResponse().getSourceAsMap().get(TENANT_ID_FIELD_KEY));
    }

    @Test
    void test_isGlobalResource_whenGlobalResourceEnabled() throws IOException {
        testInitialize();
        aosOpenSearchClient.openSearchAsyncClient = mockOpenSearchAsyncClient;
        aosOpenSearchClient.setGlobalTenantId(TEST_GLOBAL_TENANT_ID);
        aosOpenSearchClient.setGlobalResourceCacheTTL(TEST_GLOBAL_RESOURCE_CACHE_TTL);
        GetDataObjectRequest request = mock(GetDataObjectRequest.class);
        when(request.index()).thenReturn(TEST_INDEX);
        when(request.id()).thenReturn(TEST_ID);
        when(request.tenantId()).thenReturn(TEST_TENANT_ID);
        Map<String, Object> source = new HashMap<>();
        source.put(TENANT_ID_FIELD_KEY, TEST_GLOBAL_TENANT_ID);
        GetResponse<Map<String, Object>> getResponse = new GetResponse.Builder<Map<String, Object>>().index(TEST_INDEX)
            .id("mockId")
            .found(true)
            .source(source)
            .build();
        when(mockOpenSearchAsyncClient.get(any(GetRequest.class), any(Class.class))).thenReturn(
            CompletableFuture.completedFuture(getResponse)
        );

        boolean result = aosOpenSearchClient.isGlobalResource(TEST_INDEX, TEST_ID, testThreadPool.executor(TEST_THREAD_POOL), true)
            .toCompletableFuture()
            .join();
        assertTrue(result);
    }

    @Test
    void test_isGlobalResource_whenGlobalResourceDisabled() throws IOException {
        aosOpenSearchClient.openSearchAsyncClient = mockOpenSearchAsyncClient;
        GetDataObjectRequest request = mock(GetDataObjectRequest.class);
        when(request.index()).thenReturn(TEST_INDEX);
        when(request.id()).thenReturn(TEST_ID);
        when(request.tenantId()).thenReturn(TEST_TENANT_ID);
        Map<String, Object> source = new HashMap<>();
        source.put(TENANT_ID_FIELD_KEY, TEST_TENANT_ID);
        GetResponse<Map<String, Object>> getResponse = new GetResponse.Builder<Map<String, Object>>().index(TEST_INDEX)
            .id("mockId")
            .found(true)
            .source(source)
            .build();
        when(mockOpenSearchAsyncClient.get(any(GetRequest.class), any(Class.class))).thenReturn(
            CompletableFuture.completedFuture(getResponse)
        );

        boolean result = aosOpenSearchClient.isGlobalResource(TEST_INDEX, TEST_ID, testThreadPool.executor(TEST_THREAD_POOL), true)
            .toCompletableFuture()
            .join();
        assertFalse(result);
    }
}
