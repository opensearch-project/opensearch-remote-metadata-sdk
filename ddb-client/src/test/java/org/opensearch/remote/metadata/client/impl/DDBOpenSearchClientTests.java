/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.remote.metadata.client.impl;

import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.ConditionalCheckFailedException;
import software.amazon.awssdk.services.dynamodb.model.DeleteItemRequest;
import software.amazon.awssdk.services.dynamodb.model.DeleteItemResponse;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.GetItemResponse;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.PutItemResponse;
import software.amazon.awssdk.services.dynamodb.model.UpdateItemRequest;
import software.amazon.awssdk.services.dynamodb.model.UpdateItemResponse;

import org.opensearch.OpenSearchStatusException;
import org.opensearch.action.DocWriteResponse;
import org.opensearch.action.delete.DeleteResponse;
import org.opensearch.action.get.GetResponse;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.action.update.UpdateResponse;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.concurrent.OpenSearchExecutors;
import org.opensearch.common.xcontent.LoggingDeprecationHandler;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.DeprecationHandler;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.remote.metadata.client.BulkDataObjectRequest;
import org.opensearch.remote.metadata.client.BulkDataObjectResponse;
import org.opensearch.remote.metadata.client.DeleteDataObjectRequest;
import org.opensearch.remote.metadata.client.DeleteDataObjectResponse;
import org.opensearch.remote.metadata.client.GetDataObjectRequest;
import org.opensearch.remote.metadata.client.GetDataObjectResponse;
import org.opensearch.remote.metadata.client.PutDataObjectRequest;
import org.opensearch.remote.metadata.client.PutDataObjectResponse;
import org.opensearch.remote.metadata.client.SdkClient;
import org.opensearch.remote.metadata.client.SearchDataObjectRequest;
import org.opensearch.remote.metadata.client.SearchDataObjectResponse;
import org.opensearch.remote.metadata.client.UpdateDataObjectRequest;
import org.opensearch.remote.metadata.client.UpdateDataObjectResponse;
import org.opensearch.remote.metadata.common.CommonValue;
import org.opensearch.remote.metadata.common.ComplexDataObject;
import org.opensearch.remote.metadata.common.SdkClientUtils;
import org.opensearch.remote.metadata.common.TestDataObject;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.threadpool.ScalingExecutorBuilder;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;

import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import static org.opensearch.core.xcontent.XContentParserUtils.ensureExpectedToken;
import static org.opensearch.remote.metadata.client.impl.DDBOpenSearchClient.simulateOpenSearchResponse;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class DDBOpenSearchClientTests {

    private static final String RANGE_KEY = "_id";
    private static final String HASH_KEY = "_tenant_id";
    private static final String SEQ_NUM = "_seq_no";
    private static final String SOURCE = "_source";

    private static final String TEST_ID = "123";
    private static final String TENANT_ID_FIELD = "tenant_id";
    private static final String TENANT_ID = "TEST_TENANT_ID";
    private static final String TEST_INDEX = "test_index";
    private static final String TEST_INDEX_2 = "test_index_2";
    private static final String TEST_SYSTEM_INDEX = ".test_index";
    private static final String TEST_THREAD_POOL = "test_pool";
    private SdkClient sdkClient;

    @Mock
    private DynamoDbAsyncClient dynamoDbAsyncClient;
    @Mock
    private AOSOpenSearchClient aosOpenSearchClient;
    @Captor
    private ArgumentCaptor<PutItemRequest> putItemRequestArgumentCaptor;
    @Captor
    private ArgumentCaptor<GetItemRequest> getItemRequestArgumentCaptor;
    @Captor
    private ArgumentCaptor<DeleteItemRequest> deleteItemRequestArgumentCaptor;
    @Captor
    private ArgumentCaptor<UpdateItemRequest> updateItemRequestArgumentCaptor;
    @Captor
    private ArgumentCaptor<SearchDataObjectRequest> searchDataObjectRequestArgumentCaptor;
    private TestDataObject testDataObject;

    private static TestThreadPool testThreadPool = new TestThreadPool(
        DDBOpenSearchClientTests.class.getName(),
        new ScalingExecutorBuilder(
            TEST_THREAD_POOL,
            1,
            Math.max(1, OpenSearchExecutors.allocatedProcessors(Settings.EMPTY) - 1),
            TimeValue.timeValueMinutes(1),
            TEST_THREAD_POOL
        )
    );

    @AfterAll
    public static void cleanup() {
        ThreadPool.terminate(testThreadPool, 500, TimeUnit.MILLISECONDS);
    }

    @BeforeEach
    public void setup() {
        MockitoAnnotations.openMocks(this);

        sdkClient = SdkClientFactory.wrapSdkClientDelegate(
            new DDBOpenSearchClient(dynamoDbAsyncClient, aosOpenSearchClient, TENANT_ID_FIELD),
            true
        );
        testDataObject = new TestDataObject("foo");
        when(dynamoDbAsyncClient.getItem(any(GetItemRequest.class))).thenReturn(
            CompletableFuture.completedFuture(GetItemResponse.builder().build())
        );
    }

    @Test
    public void testPutDataObject_HappyCase() throws IOException {
        PutDataObjectRequest putRequest = PutDataObjectRequest.builder()
            .index(TEST_INDEX)
            .id(TEST_ID)
            .tenantId(TENANT_ID)
            .overwriteIfExists(false)
            .tenantId(TENANT_ID)
            .dataObject(testDataObject)
            .build();
        when(dynamoDbAsyncClient.putItem(any(PutItemRequest.class))).thenReturn(
            CompletableFuture.completedFuture(PutItemResponse.builder().build())
        );
        PutDataObjectResponse response = sdkClient.putDataObjectAsync(putRequest, testThreadPool.executor(TEST_THREAD_POOL))
            .toCompletableFuture()
            .join();
        verify(dynamoDbAsyncClient).putItem(putItemRequestArgumentCaptor.capture());
        assertEquals(TEST_ID, response.id());

        IndexResponse indexActionResponse = IndexResponse.fromXContent(response.parser());
        assertEquals(TEST_ID, indexActionResponse.getId());
        assertEquals(DocWriteResponse.Result.CREATED, indexActionResponse.getResult());
        assertEquals(0, indexActionResponse.getSeqNo());

        PutItemRequest putItemRequest = putItemRequestArgumentCaptor.getValue();
        assertEquals(TEST_INDEX, putItemRequest.tableName());
        assertEquals(TEST_ID, putItemRequest.item().get(RANGE_KEY).s());
        assertEquals(TENANT_ID, putItemRequest.item().get(HASH_KEY).s());
        assertEquals("0", putItemRequest.item().get(SEQ_NUM).n());
        assertEquals("foo", putItemRequest.item().get(SOURCE).m().get("data").s());
    }

    @Test
    public void testPutDataObject_ExistingDocument_UpdatesSequenceNumber() throws IOException {
        PutDataObjectRequest putRequest = PutDataObjectRequest.builder()
            .index(TEST_INDEX)
            .id(TEST_ID)
            .tenantId(TENANT_ID)
            .dataObject(testDataObject)
            .build();
        when(dynamoDbAsyncClient.getItem(any(GetItemRequest.class))).thenReturn(
            CompletableFuture.completedFuture(
                GetItemResponse.builder().item(Map.of(SEQ_NUM, AttributeValue.builder().n("5").build())).build()
            )
        );
        when(dynamoDbAsyncClient.putItem(any(PutItemRequest.class))).thenReturn(
            CompletableFuture.completedFuture(PutItemResponse.builder().build())
        );
        PutDataObjectResponse response = sdkClient.putDataObjectAsync(putRequest, testThreadPool.executor(TEST_THREAD_POOL))
            .toCompletableFuture()
            .join();
        verify(dynamoDbAsyncClient).putItem(putItemRequestArgumentCaptor.capture());
        PutItemRequest putItemRequest = putItemRequestArgumentCaptor.getValue();
        IndexResponse indexActionResponse = IndexResponse.fromXContent(response.parser());
        assertEquals(6, indexActionResponse.getSeqNo());
        assertEquals("6", putItemRequest.item().get(SEQ_NUM).n());
    }

    @Test
    public void testPutDataObject_ExistingDocument_DisableOverwrite() {
        PutDataObjectRequest putRequest = PutDataObjectRequest.builder()
            .index(TEST_INDEX)
            .id(TEST_ID)
            .tenantId(TENANT_ID)
            .overwriteIfExists(false)
            .dataObject(testDataObject)
            .build();
        when(dynamoDbAsyncClient.getItem(any(GetItemRequest.class))).thenReturn(
            CompletableFuture.completedFuture(
                GetItemResponse.builder().item(Map.of(SEQ_NUM, AttributeValue.builder().n("5").build())).build()
            )
        );
        when(dynamoDbAsyncClient.putItem(any(PutItemRequest.class))).thenReturn(
            CompletableFuture.completedFuture(PutItemResponse.builder().build())
        );
        CompletableFuture<PutDataObjectResponse> response = sdkClient.putDataObjectAsync(
            putRequest,
            testThreadPool.executor(TEST_THREAD_POOL)
        ).toCompletableFuture();
        CompletionException ce = assertThrows(CompletionException.class, () -> response.join());
        assertEquals(OpenSearchStatusException.class, ce.getCause().getClass());
    }

    @Test
    public void testPutDataObject_WithComplexData() {
        ComplexDataObject complexDataObject = ComplexDataObject.builder()
            .testString("testString")
            .testNumber(123)
            .testBool(true)
            .testList(Arrays.asList("123", "hello", null))
            .testObject(testDataObject)
            .build();
        PutDataObjectRequest putRequest = PutDataObjectRequest.builder()
            .index(TEST_INDEX)
            .id(TEST_ID)
            .tenantId(TENANT_ID)
            .dataObject(complexDataObject)
            .build();
        when(dynamoDbAsyncClient.putItem(any(PutItemRequest.class))).thenReturn(
            CompletableFuture.completedFuture(PutItemResponse.builder().build())
        );
        sdkClient.putDataObjectAsync(putRequest, testThreadPool.executor(TEST_THREAD_POOL)).toCompletableFuture().join();
        verify(dynamoDbAsyncClient).putItem(putItemRequestArgumentCaptor.capture());
        PutItemRequest putItemRequest = putItemRequestArgumentCaptor.getValue();
        assertEquals("testString", putItemRequest.item().get(SOURCE).m().get("testString").s());
        assertEquals("123", putItemRequest.item().get(SOURCE).m().get("testNumber").n());
        assertEquals(true, putItemRequest.item().get(SOURCE).m().get("testBool").bool());
        assertEquals("123", putItemRequest.item().get(SOURCE).m().get("testList").l().get(0).s());
        assertEquals("hello", putItemRequest.item().get(SOURCE).m().get("testList").l().get(1).s());
        assertEquals(null, putItemRequest.item().get(SOURCE).m().get("testList").l().get(2).s());
        assertEquals("foo", putItemRequest.item().get(SOURCE).m().get("testObject").m().get("data").s());
        assertEquals(TENANT_ID, putItemRequest.item().get(SOURCE).m().get(CommonValue.TENANT_ID_FIELD_KEY).s());
    }

    @Test
    public void testPutDataObject_NullId_SetsDefaultTenantId() {
        PutDataObjectRequest putRequest = PutDataObjectRequest.builder()
            .index(TEST_INDEX)
            .tenantId(TENANT_ID)
            .dataObject(testDataObject)
            .build();
        when(dynamoDbAsyncClient.putItem(any(PutItemRequest.class))).thenReturn(
            CompletableFuture.completedFuture(PutItemResponse.builder().build())
        );
        PutDataObjectResponse response = sdkClient.putDataObjectAsync(putRequest, testThreadPool.executor(TEST_THREAD_POOL))
            .toCompletableFuture()
            .join();
        verify(dynamoDbAsyncClient).putItem(putItemRequestArgumentCaptor.capture());

        PutItemRequest putItemRequest = putItemRequestArgumentCaptor.getValue();
        assertNotNull(putItemRequest.item().get(RANGE_KEY).s());
        assertNotNull(response.id());
    }

    @Test
    public void testPutDataObject_DDBException_ThrowsException() {
        PutDataObjectRequest putRequest = PutDataObjectRequest.builder()
            .index(TEST_INDEX)
            .id(TEST_ID)
            .tenantId(TENANT_ID)
            .dataObject(testDataObject)
            .build();
        when(dynamoDbAsyncClient.putItem(any(PutItemRequest.class))).thenReturn(
            CompletableFuture.failedFuture(new RuntimeException("Test exception"))
        );
        CompletableFuture<PutDataObjectResponse> future = sdkClient.putDataObjectAsync(
            putRequest,
            testThreadPool.executor(TEST_THREAD_POOL)
        ).toCompletableFuture();

        CompletionException ce = assertThrows(CompletionException.class, () -> future.join());
        assertEquals(RuntimeException.class, ce.getCause().getClass());
    }

    @Test
    public void testPutDataObject_InvalidId_ThrowsBadRequest() {
        String longId = String.join("", Collections.nCopies(513, "a"));
        PutDataObjectRequest putRequest = PutDataObjectRequest.builder()
            .index(TEST_INDEX)
            .id(longId)
            .tenantId(TENANT_ID)
            .dataObject(testDataObject)
            .build();
        OpenSearchStatusException ose = assertThrows(
            OpenSearchStatusException.class,
            () -> sdkClient.putDataObjectAsync(putRequest, testThreadPool.executor(TEST_THREAD_POOL))
        );
        assertEquals(RestStatus.BAD_REQUEST, ose.status());
        assertTrue(ose.getMessage().contains("is too long, must be no longer than 512 bytes but was: 513"));
    }

    @Test
    public void testPutDataObject_InvalidDataObject_ThrowsBadRequest() {
        ToXContentObject invalidDataObject = new ToXContentObject() {
            public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
                throw new IOException("Test IOException");
            }
        };
        PutDataObjectRequest putRequest = PutDataObjectRequest.builder()
            .index(TEST_INDEX)
            .id(TEST_ID)
            .tenantId(TENANT_ID)
            .dataObject(invalidDataObject)
            .build();
        OpenSearchStatusException ose = assertThrows(
            OpenSearchStatusException.class,
            () -> sdkClient.putDataObjectAsync(putRequest, testThreadPool.executor(TEST_THREAD_POOL))
        );
        assertEquals(RestStatus.BAD_REQUEST, ose.status());
        assertEquals("Request body validation failed.", ose.getMessage());
        assertTrue(ose.getCause() instanceof IOException);
        assertEquals("Test IOException", ose.getCause().getMessage());
    }

    @Test
    public void testGetDataObject_HappyCase() throws IOException {
        GetDataObjectRequest getRequest = GetDataObjectRequest.builder().index(TEST_INDEX).id(TEST_ID).tenantId(TENANT_ID).build();
        GetItemResponse getItemResponse = GetItemResponse.builder()
            .item(
                Map.ofEntries(
                    Map.entry(
                        SOURCE,
                        AttributeValue.builder().m(Map.ofEntries(Map.entry("data", AttributeValue.builder().s("foo").build()))).build()
                    ),
                    Map.entry(SEQ_NUM, AttributeValue.builder().n("1").build())
                )
            )
            .build();
        when(dynamoDbAsyncClient.getItem(any(GetItemRequest.class))).thenReturn(CompletableFuture.completedFuture(getItemResponse));
        GetDataObjectResponse response = sdkClient.getDataObjectAsync(getRequest, testThreadPool.executor(TEST_THREAD_POOL))
            .toCompletableFuture()
            .join();
        verify(dynamoDbAsyncClient).getItem(getItemRequestArgumentCaptor.capture());
        GetItemRequest getItemRequest = getItemRequestArgumentCaptor.getValue();
        assertEquals(TEST_INDEX, getItemRequest.tableName());
        assertEquals(TENANT_ID, getItemRequest.key().get(HASH_KEY).s());
        assertEquals(TEST_ID, getItemRequest.key().get(RANGE_KEY).s());
        assertEquals(TEST_ID, response.id());
        assertEquals("foo", response.source().get("data"));
        XContentParser parser = response.parser();
        GetResponse getResponse = GetResponse.fromXContent(parser);
        assertEquals(1, getResponse.getSeqNo());
        XContentParser dataParser = XContentHelper.createParser(
            NamedXContentRegistry.EMPTY,
            LoggingDeprecationHandler.INSTANCE,
            getResponse.getSourceAsBytesRef(),
            XContentType.JSON
        );
        ensureExpectedToken(XContentParser.Token.START_OBJECT, dataParser.nextToken(), dataParser);
        assertEquals("foo", TestDataObject.parse(dataParser).data());
    }

    @Test
    public void testGetDataObject_ComplexDataObject() throws IOException {
        GetDataObjectRequest getRequest = GetDataObjectRequest.builder().index(TEST_INDEX).id(TEST_ID).tenantId(TENANT_ID).build();
        GetItemResponse getItemResponse = GetItemResponse.builder()
            .item(
                Map.ofEntries(
                    Map.entry(SOURCE, AttributeValue.builder().m(getComplexDataSource()).build()),
                    Map.entry(SEQ_NUM, AttributeValue.builder().n("1").build())
                )
            )
            .build();
        when(dynamoDbAsyncClient.getItem(any(GetItemRequest.class))).thenReturn(CompletableFuture.completedFuture(getItemResponse));
        GetDataObjectResponse response = sdkClient.getDataObjectAsync(getRequest, testThreadPool.executor(TEST_THREAD_POOL))
            .toCompletableFuture()
            .join();
        verify(dynamoDbAsyncClient).getItem(getItemRequestArgumentCaptor.capture());

        GetResponse getResponse = GetResponse.fromXContent(response.parser());
        XContentParser parser = JsonXContent.jsonXContent.createParser(
            NamedXContentRegistry.EMPTY,
            DeprecationHandler.IGNORE_DEPRECATIONS,
            getResponse.getSourceAsString()
        );
        ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser);
        ComplexDataObject complexDataObject = ComplexDataObject.parse(parser);
        assertEquals("testString", complexDataObject.getTestString());
        assertEquals(123, complexDataObject.getTestNumber());
        assertEquals("testString", complexDataObject.getTestList().get(0));
        assertEquals("foo", complexDataObject.getTestObject().data());
        assertEquals(true, complexDataObject.isTestBool());
    }

    @Test
    public void testGetDataObject_NoExistingDoc() throws IOException {
        GetDataObjectRequest getRequest = GetDataObjectRequest.builder().index(TEST_INDEX).id(TEST_ID).tenantId(TENANT_ID).build();
        GetItemResponse getItemResponse = GetItemResponse.builder().build();
        when(dynamoDbAsyncClient.getItem(any(GetItemRequest.class))).thenReturn(CompletableFuture.completedFuture(getItemResponse));
        GetDataObjectResponse response = sdkClient.getDataObjectAsync(getRequest, testThreadPool.executor(TEST_THREAD_POOL))
            .toCompletableFuture()
            .join();
        assertEquals(TEST_ID, response.id());
        assertTrue(response.source().isEmpty());
        assertFalse(GetResponse.fromXContent(response.parser()).isExists());
    }

    @Test
    public void testGetDataObject_UseDefaultTenantIdIfNull() {
        GetDataObjectRequest getRequest = GetDataObjectRequest.builder().index(TEST_INDEX).id(TEST_ID).build();
        GetItemResponse getItemResponse = GetItemResponse.builder().build();
        when(dynamoDbAsyncClient.getItem(any(GetItemRequest.class))).thenReturn(CompletableFuture.completedFuture(getItemResponse));
        sdkClient.getDataObjectAsync(getRequest, testThreadPool.executor(TEST_THREAD_POOL)).toCompletableFuture().join();
        verify(dynamoDbAsyncClient).getItem(getItemRequestArgumentCaptor.capture());
        GetItemRequest getItemRequest = getItemRequestArgumentCaptor.getValue();
        assertEquals("DEFAULT_TENANT", getItemRequest.key().get(HASH_KEY).s());
    }

    @Test
    public void testGetDataObject_DDBException_ThrowsOSException() {
        GetDataObjectRequest getRequest = GetDataObjectRequest.builder().index(TEST_INDEX).id(TEST_ID).tenantId(TENANT_ID).build();
        when(dynamoDbAsyncClient.getItem(any(GetItemRequest.class))).thenReturn(
            CompletableFuture.failedFuture(new RuntimeException("Test exception"))
        );
        CompletableFuture<GetDataObjectResponse> future = sdkClient.getDataObjectAsync(
            getRequest,
            testThreadPool.executor(TEST_THREAD_POOL)
        ).toCompletableFuture();
        CompletionException ce = assertThrows(CompletionException.class, () -> future.join());
        assertEquals(RuntimeException.class, ce.getCause().getClass());
    }

    @Test
    public void testDeleteDataObject_HappyCase() throws IOException {
        DeleteDataObjectRequest deleteRequest = DeleteDataObjectRequest.builder().id(TEST_ID).index(TEST_INDEX).tenantId(TENANT_ID).build();
        when(dynamoDbAsyncClient.deleteItem(deleteItemRequestArgumentCaptor.capture())).thenReturn(
            CompletableFuture.completedFuture(
                DeleteItemResponse.builder().attributes(Map.of(SEQ_NUM, AttributeValue.builder().n("5").build())).build()
            )
        );
        DeleteDataObjectResponse deleteResponse = sdkClient.deleteDataObjectAsync(deleteRequest, testThreadPool.executor(TEST_THREAD_POOL))
            .toCompletableFuture()
            .join();
        DeleteItemRequest deleteItemRequest = deleteItemRequestArgumentCaptor.getValue();
        assertEquals(TEST_INDEX, deleteItemRequest.tableName());
        assertEquals(TENANT_ID, deleteItemRequest.key().get(HASH_KEY).s());
        assertEquals(TEST_ID, deleteItemRequest.key().get(RANGE_KEY).s());
        assertEquals(TEST_ID, deleteResponse.id());

        DeleteResponse deleteActionResponse = DeleteResponse.fromXContent(deleteResponse.parser());
        assertEquals(TEST_ID, deleteActionResponse.getId());
        assertEquals(6, deleteActionResponse.getSeqNo());
        assertEquals(DocWriteResponse.Result.DELETED, deleteActionResponse.getResult());
        assertEquals(0, deleteActionResponse.getShardInfo().getFailed());
        assertEquals(0, deleteActionResponse.getShardInfo().getSuccessful());
        assertEquals(0, deleteActionResponse.getShardInfo().getTotal());
    }

    @Test
    public void testUpdateDataObjectAsync_HappyCase() {
        UpdateDataObjectRequest updateRequest = UpdateDataObjectRequest.builder()
            .id(TEST_ID)
            .index(TEST_INDEX)
            .tenantId(TENANT_ID)
            .retryOnConflict(1)
            .dataObject(testDataObject)
            .build();
        GetItemResponse getItemResponse = GetItemResponse.builder()
            .item(
                Map.ofEntries(
                    Map.entry(SEQ_NUM, AttributeValue.builder().n("0").build()),
                    Map.entry(
                        SOURCE,
                        AttributeValue.builder().m(Map.of("old_key", AttributeValue.builder().s("old_value").build())).build()
                    )
                )
            )
            .build();
        when(dynamoDbAsyncClient.getItem(any(GetItemRequest.class))).thenReturn(CompletableFuture.completedFuture(getItemResponse));
        when(dynamoDbAsyncClient.updateItem(updateItemRequestArgumentCaptor.capture())).thenReturn(
            CompletableFuture.completedFuture(UpdateItemResponse.builder().build())
        );
        UpdateDataObjectResponse updateResponse = sdkClient.updateDataObjectAsync(updateRequest, testThreadPool.executor(TEST_THREAD_POOL))
            .toCompletableFuture()
            .join();
        assertEquals(TEST_ID, updateResponse.id());
        UpdateItemRequest updateItemRequest = updateItemRequestArgumentCaptor.getValue();
        assertEquals(TEST_ID, updateRequest.id());
        assertEquals(TEST_INDEX, updateItemRequest.tableName());
        assertEquals(TEST_ID, updateItemRequest.key().get(RANGE_KEY).s());
        assertEquals(TENANT_ID, updateItemRequest.key().get(HASH_KEY).s());
        assertEquals("foo", updateItemRequest.expressionAttributeValues().get(":source").m().get("data").s());
        assertEquals("old_value", updateItemRequest.expressionAttributeValues().get(":source").m().get("old_key").s());
    }

    @Test
    public void testUpdateDataObjectAsync_HappyCaseWithMap() throws Exception {
        UpdateDataObjectRequest updateRequest = UpdateDataObjectRequest.builder()
            .id(TEST_ID)
            .index(TEST_INDEX)
            .tenantId(TENANT_ID)
            .dataObject(Map.of("foo", "bar"))
            .ifSeqNo(10)
            .ifPrimaryTerm(10)
            .build();
        GetItemResponse getItemResponse = GetItemResponse.builder()
            .item(
                Map.ofEntries(
                    Map.entry(SEQ_NUM, AttributeValue.builder().n("0").build()),
                    Map.entry(
                        SOURCE,
                        AttributeValue.builder().m(Map.of("old_key", AttributeValue.builder().s("old_value").build())).build()
                    )
                )
            )
            .build();
        when(dynamoDbAsyncClient.getItem(any(GetItemRequest.class))).thenReturn(CompletableFuture.completedFuture(getItemResponse));
        when(dynamoDbAsyncClient.updateItem(updateItemRequestArgumentCaptor.capture())).thenReturn(
            CompletableFuture.completedFuture(
                UpdateItemResponse.builder().attributes(Map.of(SEQ_NUM, AttributeValue.builder().n("5").build())).build()
            )
        );
        UpdateDataObjectResponse updateResponse = sdkClient.updateDataObjectAsync(updateRequest, testThreadPool.executor(TEST_THREAD_POOL))
            .toCompletableFuture()
            .join();
        assertEquals(TEST_ID, updateResponse.id());
        UpdateItemRequest updateItemRequest = updateItemRequestArgumentCaptor.getValue();
        assertEquals(TEST_INDEX, updateItemRequest.tableName());
        assertEquals(TEST_ID, updateItemRequest.key().get(RANGE_KEY).s());
        assertEquals(TENANT_ID, updateItemRequest.key().get(HASH_KEY).s());
        assertTrue(updateItemRequest.expressionAttributeNames().containsKey("#seqNo"));
        assertTrue(updateItemRequest.expressionAttributeNames().containsKey("#source"));
        assertTrue(updateItemRequest.expressionAttributeValues().containsKey(":incr"));
        assertTrue(updateItemRequest.expressionAttributeValues().containsKey(":source"));
        assertEquals("bar", updateItemRequest.expressionAttributeValues().get(":source").m().get("foo").s());
        assertEquals("old_value", updateItemRequest.expressionAttributeValues().get(":source").m().get("old_key").s());
        assertTrue(updateItemRequest.expressionAttributeValues().containsKey(":currentSeqNo"));
        assertNotNull(updateItemRequest.conditionExpression());
        UpdateResponse response = UpdateResponse.fromXContent(updateResponse.parser());
        assertEquals(5, response.getSeqNo());

    }

    @Test
    public void testUpdateDataObjectAsync_NullTenantId_UsesDefaultTenantId() {
        UpdateDataObjectRequest updateRequest = UpdateDataObjectRequest.builder()
            .id(TEST_ID)
            .index(TEST_INDEX)
            .tenantId(TENANT_ID)
            .dataObject(testDataObject)
            .build();
        GetItemResponse getItemResponse = GetItemResponse.builder()
            .item(
                Map.ofEntries(
                    Map.entry(SEQ_NUM, AttributeValue.builder().n("0").build()),
                    Map.entry(SOURCE, AttributeValue.builder().m(Collections.emptyMap()).build())
                )
            )
            .build();
        when(dynamoDbAsyncClient.getItem(any(GetItemRequest.class))).thenReturn(CompletableFuture.completedFuture(getItemResponse));
        when(dynamoDbAsyncClient.updateItem(updateItemRequestArgumentCaptor.capture())).thenReturn(
            CompletableFuture.completedFuture(UpdateItemResponse.builder().build())
        );
        sdkClient.updateDataObjectAsync(updateRequest, testThreadPool.executor(TEST_THREAD_POOL)).toCompletableFuture().join();
        UpdateItemRequest updateItemRequest = updateItemRequestArgumentCaptor.getValue();
        assertEquals(TENANT_ID, updateItemRequest.key().get(HASH_KEY).s());
    }

    @Test
    public void testUpdateDataObject_DDBException_ThrowsException() {
        UpdateDataObjectRequest updateRequest = UpdateDataObjectRequest.builder()
            .index(TEST_INDEX)
            .id(TEST_ID)
            .tenantId(TENANT_ID)
            .dataObject(testDataObject)
            .build();
        when(dynamoDbAsyncClient.getItem(any(GetItemRequest.class))).thenReturn(
            CompletableFuture.failedFuture(new RuntimeException("Test exception"))
        );
        CompletableFuture<UpdateDataObjectResponse> future = sdkClient.updateDataObjectAsync(
            updateRequest,
            testThreadPool.executor(TEST_THREAD_POOL)
        ).toCompletableFuture();

        CompletionException ce = assertThrows(CompletionException.class, () -> future.join());
        assertEquals(RuntimeException.class, ce.getCause().getClass());
    }

    @Test
    public void testUpdateDataObject_VersionCheck() {
        UpdateDataObjectRequest updateRequest = UpdateDataObjectRequest.builder()
            .index(TEST_INDEX)
            .id(TEST_ID)
            .tenantId(TENANT_ID)
            .dataObject(testDataObject)
            .ifSeqNo(5)
            .ifPrimaryTerm(2)
            .build();

        GetItemResponse getItemResponse = GetItemResponse.builder()
            .item(
                Map.ofEntries(
                    Map.entry(SEQ_NUM, AttributeValue.builder().n("0").build()),
                    Map.entry(SOURCE, AttributeValue.builder().m(Collections.emptyMap()).build())
                )
            )
            .build();
        when(dynamoDbAsyncClient.getItem(any(GetItemRequest.class))).thenReturn(CompletableFuture.completedFuture(getItemResponse));
        ConditionalCheckFailedException conflictException = ConditionalCheckFailedException.builder().build();
        when(dynamoDbAsyncClient.updateItem(updateItemRequestArgumentCaptor.capture())).thenReturn(
            CompletableFuture.failedFuture(conflictException)
        );

        CompletableFuture<UpdateDataObjectResponse> future = sdkClient.updateDataObjectAsync(
            updateRequest,
            testThreadPool.executor(TEST_THREAD_POOL)
        ).toCompletableFuture();

        CompletionException ce = assertThrows(CompletionException.class, () -> future.join());
        Throwable cause = ce.getCause();
        assertEquals(OpenSearchStatusException.class, cause.getClass());
        assertEquals(RestStatus.CONFLICT, ((OpenSearchStatusException) cause).status());
    }

    @Test
    public void updateDataObjectAsync_VersionCheckRetrySuccess() {
        UpdateDataObjectRequest updateRequest = UpdateDataObjectRequest.builder()
            .id(TEST_ID)
            .index(TEST_INDEX)
            .tenantId(TENANT_ID)
            .retryOnConflict(1)
            .dataObject(testDataObject)
            .build();
        GetItemResponse getItemResponse = GetItemResponse.builder()
            .item(
                Map.ofEntries(
                    Map.entry(SEQ_NUM, AttributeValue.builder().n("0").build()),
                    Map.entry(SOURCE, AttributeValue.builder().m(Collections.emptyMap()).build())
                )
            )
            .build();
        when(dynamoDbAsyncClient.getItem(any(GetItemRequest.class))).thenReturn(CompletableFuture.completedFuture(getItemResponse));
        ConditionalCheckFailedException conflictException = ConditionalCheckFailedException.builder().build();
        // throw conflict exception on first time, return on second time, throw on third time (never get here)
        when(dynamoDbAsyncClient.updateItem(updateItemRequestArgumentCaptor.capture())).thenReturn(
            CompletableFuture.failedFuture(conflictException)
        )
            .thenReturn(CompletableFuture.completedFuture(UpdateItemResponse.builder().build()))
            .thenReturn(CompletableFuture.failedFuture(conflictException));
        UpdateDataObjectResponse updateResponse = sdkClient.updateDataObjectAsync(updateRequest, testThreadPool.executor(TEST_THREAD_POOL))
            .toCompletableFuture()
            .join();
        assertEquals(TEST_ID, updateResponse.id());
        UpdateItemRequest updateItemRequest = updateItemRequestArgumentCaptor.getValue();
        assertEquals(TEST_ID, updateRequest.id());
        assertEquals(TEST_INDEX, updateItemRequest.tableName());
        assertEquals(TEST_ID, updateItemRequest.key().get(RANGE_KEY).s());
        assertEquals(TENANT_ID, updateItemRequest.key().get(HASH_KEY).s());
        assertEquals("foo", updateItemRequest.expressionAttributeValues().get(":source").m().get("data").s());
    }

    @Test
    public void updateDataObjectAsync_VersionCheckRetryFailure() {
        UpdateDataObjectRequest updateRequest = UpdateDataObjectRequest.builder()
            .id(TEST_ID)
            .index(TEST_INDEX)
            .tenantId(TENANT_ID)
            .retryOnConflict(1)
            .dataObject(testDataObject)
            .build();
        GetItemResponse getItemResponse = GetItemResponse.builder()
            .item(
                Map.ofEntries(
                    Map.entry(SEQ_NUM, AttributeValue.builder().n("0").build()),
                    Map.entry(SOURCE, AttributeValue.builder().m(Collections.emptyMap()).build())
                )
            )
            .build();
        when(dynamoDbAsyncClient.getItem(any(GetItemRequest.class))).thenReturn(CompletableFuture.completedFuture(getItemResponse));
        ConditionalCheckFailedException conflictException = ConditionalCheckFailedException.builder().build();
        // throw conflict exception on first two times, return on third time (that never executes)
        when(dynamoDbAsyncClient.updateItem(updateItemRequestArgumentCaptor.capture())).thenReturn(
            CompletableFuture.failedFuture(conflictException)
        )
            .thenReturn(CompletableFuture.failedFuture(conflictException))
            .thenReturn(CompletableFuture.completedFuture(UpdateItemResponse.builder().build()));

        CompletableFuture<UpdateDataObjectResponse> future = sdkClient.updateDataObjectAsync(
            updateRequest,
            testThreadPool.executor(TEST_THREAD_POOL)
        ).toCompletableFuture();

        CompletionException ce = assertThrows(CompletionException.class, () -> future.join());
        Throwable cause = ce.getCause();
        assertEquals(OpenSearchStatusException.class, cause.getClass());
        assertEquals(RestStatus.CONFLICT, ((OpenSearchStatusException) cause).status());
    }

    @Test
    public void testUpdateDataObject_MissingId_ThrowsBadRequest() {
        UpdateDataObjectRequest updateRequest = UpdateDataObjectRequest.builder()
            .index(TEST_INDEX)
            .id(null)  // Missing ID
            .tenantId(TENANT_ID)
            .dataObject(testDataObject)
            .build();
        OpenSearchStatusException ose = assertThrows(
            OpenSearchStatusException.class,
            () -> sdkClient.updateDataObjectAsync(updateRequest, testThreadPool.executor(TEST_THREAD_POOL))
        );
        assertEquals(RestStatus.BAD_REQUEST, ose.status());
        assertTrue(ose.getMessage().contains("id is missing"));
    }

    @Test
    public void testUpdateDataObject_InvalidDataObject_ThrowsBadRequest() {
        ToXContentObject invalidDataObject = new ToXContentObject() {
            public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
                throw new IOException("Test IOException");
            }
        };
        UpdateDataObjectRequest updateRequest = UpdateDataObjectRequest.builder()
            .index(TEST_INDEX)
            .id(TEST_ID)
            .tenantId(TENANT_ID)
            .dataObject(invalidDataObject)
            .build();
        OpenSearchStatusException ose = assertThrows(
            OpenSearchStatusException.class,
            () -> sdkClient.updateDataObjectAsync(updateRequest, testThreadPool.executor(TEST_THREAD_POOL))
        );
        assertEquals(RestStatus.BAD_REQUEST, ose.status());
        assertEquals("Request body validation failed.", ose.getMessage());
        assertTrue(ose.getCause() instanceof IOException);
        assertEquals("Test IOException", ose.getCause().getMessage());
    }

    @Test
    public void testBulkDataObject_HappyCase() {
        PutDataObjectRequest putRequest = PutDataObjectRequest.builder()
            .id(TEST_ID + "1")
            .tenantId(TENANT_ID)
            .dataObject(testDataObject)
            .build();
        UpdateDataObjectRequest updateRequest = UpdateDataObjectRequest.builder()
            .id(TEST_ID + "2")
            .tenantId(TENANT_ID)
            .dataObject(testDataObject)
            .build();
        DeleteDataObjectRequest deleteRequest = DeleteDataObjectRequest.builder().id(TEST_ID + "3").tenantId(TENANT_ID).build();
        BulkDataObjectRequest bulkRequest = BulkDataObjectRequest.builder()
            .globalIndex(TEST_INDEX)
            .build()
            .add(putRequest)
            .add(updateRequest)
            .add(deleteRequest);

        when(dynamoDbAsyncClient.putItem(any(PutItemRequest.class))).thenReturn(
            CompletableFuture.completedFuture(PutItemResponse.builder().build())
        );
        GetItemResponse getItemResponse = GetItemResponse.builder()
            .item(
                Map.ofEntries(
                    Map.entry(SOURCE, AttributeValue.builder().m(Map.of("data", AttributeValue.builder().s("foo").build())).build()),
                    Map.entry(SEQ_NUM, AttributeValue.builder().n("0").build())
                )
            )
            .build();
        when(dynamoDbAsyncClient.getItem(any(GetItemRequest.class))).thenReturn(CompletableFuture.completedFuture(getItemResponse));
        when(dynamoDbAsyncClient.updateItem(any(UpdateItemRequest.class))).thenReturn(
            CompletableFuture.completedFuture(UpdateItemResponse.builder().build())
        );
        when(dynamoDbAsyncClient.deleteItem(any(DeleteItemRequest.class))).thenReturn(
            CompletableFuture.completedFuture(DeleteItemResponse.builder().build())
        );

        BulkDataObjectResponse response = sdkClient.bulkDataObjectAsync(bulkRequest, testThreadPool.executor(TEST_THREAD_POOL))
            .toCompletableFuture()
            .join();

        assertEquals(3, response.getResponses().length);
        assertTrue(response.getResponses()[0] instanceof PutDataObjectResponse);
        assertTrue(response.getResponses()[1] instanceof UpdateDataObjectResponse);
        assertTrue(response.getResponses()[2] instanceof DeleteDataObjectResponse);

        assertEquals(TEST_ID + "1", response.getResponses()[0].id());
        assertEquals(TEST_ID + "2", response.getResponses()[1].id());
        assertEquals(TEST_ID + "3", response.getResponses()[2].id());

        assertTrue(response.getTookInMillis() >= 0);
    }

    @Test
    public void testBulkDataObject_WithFailures() {
        PutDataObjectRequest putRequest = PutDataObjectRequest.builder()
            .id(TEST_ID + "1")
            .tenantId(TENANT_ID)
            .dataObject(testDataObject)
            .build();
        UpdateDataObjectRequest updateRequest = UpdateDataObjectRequest.builder()
            .id(TEST_ID + "2")
            .tenantId(TENANT_ID)
            .dataObject(testDataObject)
            .build();
        DeleteDataObjectRequest deleteRequest = DeleteDataObjectRequest.builder().id(TEST_ID + "3").tenantId(TENANT_ID).build();
        BulkDataObjectRequest bulkRequest = BulkDataObjectRequest.builder()
            .globalIndex(TEST_INDEX)
            .build()
            .add(putRequest)
            .add(updateRequest)
            .add(deleteRequest);

        when(dynamoDbAsyncClient.putItem(any(PutItemRequest.class))).thenReturn(
            CompletableFuture.completedFuture(PutItemResponse.builder().build())
        );
        GetItemResponse getItemResponse = GetItemResponse.builder()
            .item(
                Map.ofEntries(
                    Map.entry(SOURCE, AttributeValue.builder().m(Map.of("data", AttributeValue.builder().s("foo").build())).build()),
                    Map.entry(SEQ_NUM, AttributeValue.builder().n("0").build())
                )
            )
            .build();
        when(dynamoDbAsyncClient.getItem(any(GetItemRequest.class))).thenReturn(CompletableFuture.completedFuture(getItemResponse));
        Exception cause = new OpenSearchStatusException("Update failed with conflict", RestStatus.CONFLICT);
        when(dynamoDbAsyncClient.updateItem(any(UpdateItemRequest.class))).thenReturn(CompletableFuture.failedFuture(cause));
        when(dynamoDbAsyncClient.deleteItem(any(DeleteItemRequest.class))).thenReturn(
            CompletableFuture.completedFuture(DeleteItemResponse.builder().build())
        );

        BulkDataObjectResponse response = sdkClient.bulkDataObjectAsync(bulkRequest, testThreadPool.executor(TEST_THREAD_POOL))
            .toCompletableFuture()
            .join();

        assertEquals(3, response.getResponses().length);
        assertFalse(response.getResponses()[0].isFailed());
        assertNull(response.getResponses()[0].cause());
        assertTrue(response.getResponses()[0] instanceof PutDataObjectResponse);
        assertTrue(response.getResponses()[1].isFailed());
        assertTrue(response.getResponses()[1].cause() instanceof OpenSearchStatusException);
        assertEquals("Update failed with conflict", response.getResponses()[1].cause().getMessage());
        assertEquals(RestStatus.CONFLICT, response.getResponses()[1].status());
        assertTrue(response.getResponses()[1] instanceof UpdateDataObjectResponse);
        assertFalse(response.getResponses()[2].isFailed());
        assertNull(response.getResponses()[0].cause());
        assertTrue(response.getResponses()[2] instanceof DeleteDataObjectResponse);
    }

    @Test
    public void searchDataObjectAsync_HappyCase() {
        SearchSourceBuilder searchSourceBuilder = SearchSourceBuilder.searchSource();
        SearchDataObjectRequest searchDataObjectRequest = SearchDataObjectRequest.builder()
            .indices(TEST_INDEX, TEST_INDEX_2)
            .tenantId(TENANT_ID)
            .searchSourceBuilder(searchSourceBuilder)
            .build();
        @SuppressWarnings("unchecked")
        CompletionStage<SearchDataObjectResponse> searchDataObjectResponse = mock(CompletionStage.class);
        when(aosOpenSearchClient.searchDataObjectAsync(any(), any(), anyBoolean())).thenReturn(searchDataObjectResponse);
        CompletionStage<SearchDataObjectResponse> searchResponse = sdkClient.searchDataObjectAsync(searchDataObjectRequest);

        assertEquals(searchDataObjectResponse, searchResponse);
        verify(aosOpenSearchClient).searchDataObjectAsync(searchDataObjectRequestArgumentCaptor.capture(), any(), anyBoolean());
        assertEquals(TENANT_ID, searchDataObjectRequestArgumentCaptor.getValue().tenantId());
        assertEquals(TEST_INDEX, searchDataObjectRequestArgumentCaptor.getValue().indices()[0]);
        assertEquals(TEST_INDEX_2, searchDataObjectRequestArgumentCaptor.getValue().indices()[1]);
        assertEquals(searchSourceBuilder, searchDataObjectRequestArgumentCaptor.getValue().searchSourceBuilder());
    }

    @Test
    public void searchDataObjectAsync_SystemIndex() {
        SearchSourceBuilder searchSourceBuilder = SearchSourceBuilder.searchSource();
        SearchDataObjectRequest searchDataObjectRequest = SearchDataObjectRequest.builder()
            .indices(TEST_SYSTEM_INDEX)
            .tenantId(TENANT_ID)
            .searchSourceBuilder(searchSourceBuilder)
            .build();
        @SuppressWarnings("unchecked")
        CompletionStage<SearchDataObjectResponse> searchDataObjectResponse = mock(CompletionStage.class);
        when(aosOpenSearchClient.searchDataObjectAsync(any(), any(), anyBoolean())).thenReturn(searchDataObjectResponse);
        CompletionStage<SearchDataObjectResponse> searchResponse = sdkClient.searchDataObjectAsync(searchDataObjectRequest);

        assertEquals(searchDataObjectResponse, searchResponse);
        verify(aosOpenSearchClient).searchDataObjectAsync(searchDataObjectRequestArgumentCaptor.capture(), any(), anyBoolean());
        assertEquals("test_index", searchDataObjectRequestArgumentCaptor.getValue().indices()[0]);
    }

    private Map<String, AttributeValue> getComplexDataSource() {
        return Map.ofEntries(
            Map.entry("testString", AttributeValue.builder().s("testString").build()),
            Map.entry("testNumber", AttributeValue.builder().n("123").build()),
            Map.entry("testBool", AttributeValue.builder().bool(true).build()),
            Map.entry("testList", AttributeValue.builder().l(Arrays.asList(AttributeValue.builder().s("testString").build())).build()),
            Map.entry("testObject", AttributeValue.builder().m(Map.of("data", AttributeValue.builder().s("foo").build())).build())
        );
    }

    @Test
    public void testDeleteResponseWithQuotes() throws IOException {
        // Test with double quote in ID
        String json = simulateOpenSearchResponse("test-index", "\"", null, 0L, Map.of("result", "deleted"));

        XContentParser parser = SdkClientUtils.createParser(json);
        DeleteResponse response = DeleteResponse.fromXContent(parser);

        assertEquals("\"", response.getId());
        assertEquals("test-index", response.getIndex());
        assertEquals("deleted", response.getResult().toString().toLowerCase());
    }

    @Test
    public void testGetResponseWithBackslash() throws IOException {
        // Test with backslash in ID
        String source = "{\"field\":\"value\"}";
        String json = simulateOpenSearchResponse("test-index", "\\", source, 0L, Map.of("found", true));

        XContentParser parser = SdkClientUtils.createParser(json);
        GetResponse response = GetResponse.fromXContent(parser);

        assertEquals("\\", response.getId());
        assertEquals("test-index", response.getIndex());
        assertTrue(response.isExists());
        assertNotNull(response.getSourceAsString());
        assertEquals("{\"field\":\"value\"}", response.getSourceAsString());
    }

    @Test
    public void testUpdateResponseWithSpecialChars() throws IOException {
        // Test with multiple special characters in ID
        String source = "{\"updated_field\":\"new_value\"}";
        String json = simulateOpenSearchResponse("test-index", "<>?:\"{}|+", source, 0L, Map.of("result", "updated"));

        XContentParser parser = SdkClientUtils.createParser(json);
        UpdateResponse response = UpdateResponse.fromXContent(parser);

        assertEquals("<>?:\"{}|+", response.getId());
        assertEquals("test-index", response.getIndex());
        assertEquals("updated", response.getResult().toString().toLowerCase());
    }

    @Test
    public void testIndexResponseWithUrlEncodedChars() throws IOException {
        // Test with URL-encoded characters in both index and ID
        String source = "{\"new_field\":\"test_value\"}";
        String json = simulateOpenSearchResponse(
            "test%20index",
            "%22%3C%3E", // URL encoded "<>"
            source,
            0L,
            Map.of("result", "created")
        );

        XContentParser parser = SdkClientUtils.createParser(json);
        IndexResponse response = IndexResponse.fromXContent(parser);

        assertEquals("%22%3C%3E", response.getId());
        assertEquals("test%20index", response.getIndex());
        assertEquals("created", response.getResult().toString().toLowerCase());
    }
}
