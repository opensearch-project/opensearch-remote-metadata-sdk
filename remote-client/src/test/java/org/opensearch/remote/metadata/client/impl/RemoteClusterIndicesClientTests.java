/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.remote.metadata.client.impl;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;

import org.opensearch.OpenSearchStatusException;
import org.opensearch.action.DocWriteResponse;
import org.opensearch.client.json.jackson.JacksonJsonpMapper;
import org.opensearch.client.opensearch.OpenSearchAsyncClient;
import org.opensearch.client.opensearch._types.ErrorCause;
import org.opensearch.client.opensearch._types.ErrorResponse;
import org.opensearch.client.opensearch._types.OpType;
import org.opensearch.client.opensearch._types.OpenSearchException;
import org.opensearch.client.opensearch._types.Result;
import org.opensearch.client.opensearch._types.ShardStatistics;
import org.opensearch.client.opensearch._types.query_dsl.Query;
import org.opensearch.client.opensearch.core.BulkRequest;
import org.opensearch.client.opensearch.core.BulkResponse;
import org.opensearch.client.opensearch.core.DeleteRequest;
import org.opensearch.client.opensearch.core.DeleteResponse;
import org.opensearch.client.opensearch.core.GetRequest;
import org.opensearch.client.opensearch.core.GetResponse;
import org.opensearch.client.opensearch.core.IndexRequest;
import org.opensearch.client.opensearch.core.IndexResponse;
import org.opensearch.client.opensearch.core.SearchRequest;
import org.opensearch.client.opensearch.core.SearchResponse;
import org.opensearch.client.opensearch.core.UpdateRequest;
import org.opensearch.client.opensearch.core.UpdateResponse;
import org.opensearch.client.opensearch.core.bulk.BulkResponseItem;
import org.opensearch.client.opensearch.core.bulk.OperationType;
import org.opensearch.client.opensearch.core.search.HitsMetadata;
import org.opensearch.client.opensearch.core.search.TotalHits;
import org.opensearch.client.opensearch.core.search.TotalHitsRelation;
import org.opensearch.client.transport.OpenSearchTransport;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.concurrent.OpenSearchExecutors;
import org.opensearch.common.xcontent.LoggingDeprecationHandler;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.NamedXContentRegistry;
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
import java.util.concurrent.TimeUnit;

import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import static org.opensearch.core.xcontent.XContentParserUtils.ensureExpectedToken;
import static org.opensearch.remote.metadata.common.CommonValue.TENANT_ID_FIELD_KEY;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class RemoteClusterIndicesClientTests {

    private static final String TEST_ID = "123";
    private static final String TEST_INDEX = "test_index";
    private static final String TENANT_ID_FIELD = "tenant_id";
    private static final String TEST_TENANT_ID = "xyz";
    private static final String TEST_THREAD_POOL = "test_pool";

    private static TestThreadPool testThreadPool = new TestThreadPool(
        RemoteClusterIndicesClientTests.class.getName(),
        new ScalingExecutorBuilder(
            TEST_THREAD_POOL,
            1,
            Math.max(1, OpenSearchExecutors.allocatedProcessors(Settings.EMPTY) - 1),
            TimeValue.timeValueMinutes(1),
            TEST_THREAD_POOL
        )
    );

    @Mock
    private OpenSearchAsyncClient mockedOpenSearchAsyncClient;
    private SdkClient sdkClient;

    @Mock
    private OpenSearchTransport transport;

    private TestDataObject testDataObject;

    @BeforeEach
    public void setup() {
        MockitoAnnotations.openMocks(this);

        when(mockedOpenSearchAsyncClient._transport()).thenReturn(transport);
        when(transport.jsonpMapper()).thenReturn(
            new JacksonJsonpMapper(
                new ObjectMapper().setPropertyNamingStrategy(PropertyNamingStrategies.SNAKE_CASE)
                    .setSerializationInclusion(JsonInclude.Include.NON_NULL)
            )
        );
        sdkClient = SdkClientFactory.wrapSdkClientDelegate(
            new RemoteClusterIndicesClient(mockedOpenSearchAsyncClient, TENANT_ID_FIELD),
            true
        );
        testDataObject = new TestDataObject("foo");
    }

    @AfterAll
    public static void cleanup() {
        ThreadPool.terminate(testThreadPool, 500, TimeUnit.MILLISECONDS);
    }

    @Test
    public void testPutDataObject() throws IOException {
        PutDataObjectRequest putRequest = PutDataObjectRequest.builder()
            .index(TEST_INDEX)
            .tenantId(TEST_TENANT_ID)
            .dataObject(testDataObject)
            .build();

        IndexResponse indexResponse = new IndexResponse.Builder().id(TEST_ID)
            .index(TEST_INDEX)
            .primaryTerm(0)
            .result(Result.Created)
            .seqNo(0)
            .shards(new ShardStatistics.Builder().failed(0).successful(1).total(1).build())
            .version(0)
            .build();

        ArgumentCaptor<IndexRequest<?>> indexRequestCaptor = ArgumentCaptor.forClass(IndexRequest.class);
        when(mockedOpenSearchAsyncClient.index(indexRequestCaptor.capture())).thenReturn(CompletableFuture.completedFuture(indexResponse));

        PutDataObjectResponse response = sdkClient.putDataObjectAsync(putRequest, testThreadPool.executor(TEST_THREAD_POOL))
            .toCompletableFuture()
            .join();

        assertEquals(TEST_INDEX, indexRequestCaptor.getValue().index());
        assertEquals(TEST_ID, response.id());

        org.opensearch.action.index.IndexResponse indexActionResponse = org.opensearch.action.index.IndexResponse.fromXContent(
            response.parser()
        );
        assertEquals(TEST_ID, indexActionResponse.getId());
        assertEquals(DocWriteResponse.Result.CREATED, indexActionResponse.getResult());
        assertEquals(0, indexActionResponse.getShardInfo().getFailed());
        assertEquals(1, indexActionResponse.getShardInfo().getSuccessful());
        assertEquals(1, indexActionResponse.getShardInfo().getTotal());
    }

    @Test
    public void testPutDataObject_Updated() throws IOException {
        PutDataObjectRequest putRequest = PutDataObjectRequest.builder()
            .index(TEST_INDEX)
            .id(TEST_ID)
            .tenantId(TEST_TENANT_ID)
            .overwriteIfExists(false)
            .dataObject(testDataObject)
            .build();

        IndexResponse indexResponse = new IndexResponse.Builder().id(TEST_ID)
            .index(TEST_INDEX)
            .primaryTerm(0)
            .result(Result.Updated)
            .seqNo(0)
            .shards(new ShardStatistics.Builder().failed(0).successful(1).total(1).build())
            .version(0)
            .build();

        ArgumentCaptor<IndexRequest<?>> indexRequestCaptor = ArgumentCaptor.forClass(IndexRequest.class);
        when(mockedOpenSearchAsyncClient.index(indexRequestCaptor.capture())).thenReturn(CompletableFuture.completedFuture(indexResponse));

        PutDataObjectResponse response = sdkClient.putDataObjectAsync(putRequest, testThreadPool.executor(TEST_THREAD_POOL))
            .toCompletableFuture()
            .join();

        assertEquals(TEST_INDEX, indexRequestCaptor.getValue().index());
        assertEquals(TEST_ID, indexRequestCaptor.getValue().id());
        assertEquals(OpType.Create, indexRequestCaptor.getValue().opType());
        assertEquals(TEST_ID, response.id());

        org.opensearch.action.index.IndexResponse indexActionResponse = org.opensearch.action.index.IndexResponse.fromXContent(
            response.parser()
        );
        assertEquals(TEST_ID, indexActionResponse.getId());
        assertEquals(DocWriteResponse.Result.UPDATED, indexActionResponse.getResult());
        assertEquals(0, indexActionResponse.getShardInfo().getFailed());
        assertEquals(1, indexActionResponse.getShardInfo().getSuccessful());
        assertEquals(1, indexActionResponse.getShardInfo().getTotal());
    }

    @Test
    public void testPutDataObject_Exception() throws IOException {
        PutDataObjectRequest putRequest = PutDataObjectRequest.builder()
            .index(TEST_INDEX)
            .tenantId(TEST_TENANT_ID)
            .dataObject(testDataObject)
            .build();

        ArgumentCaptor<IndexRequest<?>> indexRequestCaptor = ArgumentCaptor.forClass(IndexRequest.class);
        when(mockedOpenSearchAsyncClient.index(indexRequestCaptor.capture())).thenReturn(
            CompletableFuture.failedFuture(new IOException("test"))
        );

        CompletableFuture<PutDataObjectResponse> future = sdkClient.putDataObjectAsync(
            putRequest,
            testThreadPool.executor(TEST_THREAD_POOL)
        ).toCompletableFuture();

        CompletionException ce = assertThrows(CompletionException.class, () -> future.join());
        assertEquals(OpenSearchStatusException.class, ce.getCause().getClass());
    }

    @Test
    @SuppressWarnings({ "unchecked", "rawtypes" })
    public void testGetDataObject() throws IOException {
        GetDataObjectRequest getRequest = GetDataObjectRequest.builder().index(TEST_INDEX).id(TEST_ID).tenantId(TEST_TENANT_ID).build();

        GetResponse<?> getResponse = new GetResponse.Builder<>().index(TEST_INDEX)
            .id(TEST_ID)
            .found(true)
            .source(Map.of("data", "foo"))
            .build();

        ArgumentCaptor<GetRequest> getRequestCaptor = ArgumentCaptor.forClass(GetRequest.class);
        ArgumentCaptor<Class<Map>> mapClassCaptor = ArgumentCaptor.forClass(Class.class);
        when(mockedOpenSearchAsyncClient.get(getRequestCaptor.capture(), mapClassCaptor.capture())).thenReturn(
            CompletableFuture.completedFuture((GetResponse<Map>) getResponse)
        );

        GetDataObjectResponse response = sdkClient.getDataObjectAsync(getRequest, testThreadPool.executor(TEST_THREAD_POOL))
            .toCompletableFuture()
            .join();

        assertEquals(TEST_INDEX, getRequestCaptor.getValue().index());
        assertEquals(TEST_ID, response.id());
        assertEquals("foo", response.source().get("data"));
        XContentParser parser = response.parser();
        XContentParser dataParser = XContentHelper.createParser(
            NamedXContentRegistry.EMPTY,
            LoggingDeprecationHandler.INSTANCE,
            org.opensearch.action.get.GetResponse.fromXContent(parser).getSourceAsBytesRef(),
            XContentType.JSON
        );
        ensureExpectedToken(XContentParser.Token.START_OBJECT, dataParser.nextToken(), dataParser);
        assertEquals("foo", TestDataObject.parse(dataParser).data());
    }

    @Test
    @SuppressWarnings({ "unchecked", "rawtypes" })
    public void testGetDataObject_NotFound() throws IOException {
        GetDataObjectRequest getRequest = GetDataObjectRequest.builder().index(TEST_INDEX).id(TEST_ID).tenantId(TEST_TENANT_ID).build();

        GetResponse<?> getResponse = new GetResponse.Builder<>().index(TEST_INDEX).id(TEST_ID).found(false).build();

        ArgumentCaptor<GetRequest> getRequestCaptor = ArgumentCaptor.forClass(GetRequest.class);
        ArgumentCaptor<Class<Map>> mapClassCaptor = ArgumentCaptor.forClass(Class.class);
        when(mockedOpenSearchAsyncClient.get(getRequestCaptor.capture(), mapClassCaptor.capture())).thenReturn(
            CompletableFuture.completedFuture((GetResponse<Map>) getResponse)
        );

        GetDataObjectResponse response = sdkClient.getDataObjectAsync(getRequest, testThreadPool.executor(TEST_THREAD_POOL))
            .toCompletableFuture()
            .join();

        assertEquals(TEST_INDEX, getRequestCaptor.getValue().index());
        assertEquals(TEST_ID, response.id());
        assertTrue(response.source().isEmpty());
        assertFalse(org.opensearch.action.get.GetResponse.fromXContent(response.parser()).isExists());
    }

    @Test
    @SuppressWarnings({ "unchecked", "rawtypes" })
    public void testGetDataObject_Exception() throws IOException {
        GetDataObjectRequest getRequest = GetDataObjectRequest.builder().index(TEST_INDEX).id(TEST_ID).tenantId(TEST_TENANT_ID).build();

        ArgumentCaptor<GetRequest> getRequestCaptor = ArgumentCaptor.forClass(GetRequest.class);
        ArgumentCaptor<Class<Map>> mapClassCaptor = ArgumentCaptor.forClass(Class.class);
        when(mockedOpenSearchAsyncClient.get(getRequestCaptor.capture(), mapClassCaptor.capture())).thenReturn(
            CompletableFuture.failedFuture(new IOException("test"))
        );

        CompletableFuture<GetDataObjectResponse> future = sdkClient.getDataObjectAsync(
            getRequest,
            testThreadPool.executor(TEST_THREAD_POOL)
        ).toCompletableFuture();

        CompletionException ce = assertThrows(CompletionException.class, () -> future.join());
        assertEquals(OpenSearchStatusException.class, ce.getCause().getClass());
    }

    @Test
    public void testUpdateDataObject() throws IOException {
        UpdateDataObjectRequest updateRequest = UpdateDataObjectRequest.builder()
            .index(TEST_INDEX)
            .id(TEST_ID)
            .tenantId(TEST_TENANT_ID)
            .dataObject(testDataObject)
            .build();

        UpdateResponse<Map<String, Object>> updateResponse = new UpdateResponse.Builder<Map<String, Object>>().id(TEST_ID)
            .index(TEST_INDEX)
            .primaryTerm(0)
            .result(Result.Updated)
            .seqNo(0)
            .shards(new ShardStatistics.Builder().failed(0).successful(1).total(1).build())
            .version(0)
            .build();

        @SuppressWarnings("unchecked")
        ArgumentCaptor<UpdateRequest<Map<String, Object>, ?>> updateRequestCaptor = ArgumentCaptor.forClass(UpdateRequest.class);
        when(mockedOpenSearchAsyncClient.update(updateRequestCaptor.capture(), any())).thenReturn(
            CompletableFuture.completedFuture(updateResponse)
        );

        UpdateDataObjectResponse response = sdkClient.updateDataObjectAsync(updateRequest, testThreadPool.executor(TEST_THREAD_POOL))
            .toCompletableFuture()
            .join();

        assertEquals(TEST_INDEX, updateRequestCaptor.getValue().index());
        assertNull(updateRequestCaptor.getValue().retryOnConflict());
        assertEquals(TEST_ID, response.id());

        org.opensearch.action.update.UpdateResponse updateActionResponse = org.opensearch.action.update.UpdateResponse.fromXContent(
            response.parser()
        );
        assertEquals(TEST_ID, updateActionResponse.getId());
        assertEquals(DocWriteResponse.Result.UPDATED, updateActionResponse.getResult());
        assertEquals(0, updateActionResponse.getShardInfo().getFailed());
        assertEquals(1, updateActionResponse.getShardInfo().getSuccessful());
        assertEquals(1, updateActionResponse.getShardInfo().getTotal());
    }

    @Test
    public void testUpdateDataObjectWithMap() throws IOException {
        UpdateDataObjectRequest updateRequest = UpdateDataObjectRequest.builder()
            .index(TEST_INDEX)
            .id(TEST_ID)
            .tenantId(TEST_TENANT_ID)
            .retryOnConflict(3)
            .dataObject(Map.of("foo", "bar"))
            .build();

        UpdateResponse<Map<String, Object>> updateResponse = new UpdateResponse.Builder<Map<String, Object>>().id(TEST_ID)
            .index(TEST_INDEX)
            .primaryTerm(0)
            .result(Result.Updated)
            .seqNo(0)
            .shards(new ShardStatistics.Builder().failed(0).successful(1).total(1).build())
            .version(0)
            .build();

        @SuppressWarnings("unchecked")
        ArgumentCaptor<UpdateRequest<Map<String, Object>, ?>> updateRequestCaptor = ArgumentCaptor.forClass(UpdateRequest.class);
        when(mockedOpenSearchAsyncClient.update(updateRequestCaptor.capture(), any())).thenReturn(
            CompletableFuture.completedFuture(updateResponse)
        );

        sdkClient.updateDataObjectAsync(updateRequest, testThreadPool.executor(TEST_THREAD_POOL)).toCompletableFuture().join();

        assertEquals(TEST_INDEX, updateRequestCaptor.getValue().index());
        assertEquals(3, updateRequestCaptor.getValue().retryOnConflict().intValue());
        assertEquals(TEST_ID, updateRequestCaptor.getValue().id());
        @SuppressWarnings("unchecked")
        Map<String, Object> docMap = (Map<String, Object>) updateRequestCaptor.getValue().doc();
        assertEquals("bar", docMap.get("foo"));
    }

    @Test
    public void testUpdateDataObject_NotFound() throws IOException {
        UpdateDataObjectRequest updateRequest = UpdateDataObjectRequest.builder()
            .index(TEST_INDEX)
            .id(TEST_ID)
            .tenantId(TEST_TENANT_ID)
            .dataObject(testDataObject)
            .build();

        UpdateResponse<Map<String, Object>> updateResponse = new UpdateResponse.Builder<Map<String, Object>>().id(TEST_ID)
            .index(TEST_INDEX)
            .primaryTerm(0)
            .result(Result.Created)
            .seqNo(0)
            .shards(new ShardStatistics.Builder().failed(0).successful(1).total(1).build())
            .version(0)
            .build();

        @SuppressWarnings("unchecked")
        ArgumentCaptor<UpdateRequest<Map<String, Object>, ?>> updateRequestCaptor = ArgumentCaptor.forClass(UpdateRequest.class);
        when(mockedOpenSearchAsyncClient.update(updateRequestCaptor.capture(), any())).thenReturn(
            CompletableFuture.completedFuture(updateResponse)
        );

        UpdateDataObjectResponse response = sdkClient.updateDataObjectAsync(updateRequest, testThreadPool.executor(TEST_THREAD_POOL))
            .toCompletableFuture()
            .join();

        assertEquals(TEST_INDEX, updateRequestCaptor.getValue().index());
        assertEquals(TEST_ID, response.id());

        org.opensearch.action.update.UpdateResponse updateActionResponse = org.opensearch.action.update.UpdateResponse.fromXContent(
            response.parser()
        );
        assertEquals(TEST_ID, updateActionResponse.getId());
        assertEquals(DocWriteResponse.Result.CREATED, updateActionResponse.getResult());
        assertEquals(0, updateActionResponse.getShardInfo().getFailed());
        assertEquals(1, updateActionResponse.getShardInfo().getSuccessful());
        assertEquals(1, updateActionResponse.getShardInfo().getTotal());
    }

    @Test
    public void testtUpdateDataObject_Exception() throws IOException {
        UpdateDataObjectRequest updateRequest = UpdateDataObjectRequest.builder()
            .index(TEST_INDEX)
            .id(TEST_ID)
            .tenantId(TEST_TENANT_ID)
            .dataObject(testDataObject)
            .build();

        ArgumentCaptor<UpdateRequest<?, ?>> updateRequestCaptor = ArgumentCaptor.forClass(UpdateRequest.class);
        when(mockedOpenSearchAsyncClient.update(updateRequestCaptor.capture(), any())).thenReturn(
            CompletableFuture.failedFuture(new IOException("test"))
        );

        CompletableFuture<UpdateDataObjectResponse> future = sdkClient.updateDataObjectAsync(
            updateRequest,
            testThreadPool.executor(TEST_THREAD_POOL)
        ).toCompletableFuture();

        CompletionException ce = assertThrows(CompletionException.class, () -> future.join());
        assertEquals(OpenSearchStatusException.class, ce.getCause().getClass());
    }

    @Test
    public void testUpdateDataObject_VersionCheck() throws IOException {
        UpdateDataObjectRequest updateRequest = UpdateDataObjectRequest.builder()
            .index(TEST_INDEX)
            .id(TEST_ID)
            .tenantId(TEST_TENANT_ID)
            .dataObject(testDataObject)
            .ifSeqNo(5)
            .ifPrimaryTerm(2)
            .build();

        ArgumentCaptor<UpdateRequest<?, ?>> updateRequestCaptor = ArgumentCaptor.forClass(UpdateRequest.class);
        OpenSearchException conflictException = new OpenSearchException(
            new ErrorResponse.Builder().status(RestStatus.CONFLICT.getStatus())
                .error(new ErrorCause.Builder().type("test").reason("test").build())
                .build()
        );
        when(mockedOpenSearchAsyncClient.update(updateRequestCaptor.capture(), any())).thenReturn(
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
    public void testDeleteDataObject() throws IOException {
        DeleteDataObjectRequest deleteRequest = DeleteDataObjectRequest.builder()
            .index(TEST_INDEX)
            .id(TEST_ID)
            .tenantId(TEST_TENANT_ID)
            .build();

        DeleteResponse deleteResponse = new DeleteResponse.Builder().id(TEST_ID)
            .index(TEST_INDEX)
            .primaryTerm(0)
            .result(Result.Deleted)
            .seqNo(0)
            .shards(new ShardStatistics.Builder().failed(0).successful(2).total(2).build())
            .version(0)
            .build();

        ArgumentCaptor<DeleteRequest> deleteRequestCaptor = ArgumentCaptor.forClass(DeleteRequest.class);
        when(mockedOpenSearchAsyncClient.delete(deleteRequestCaptor.capture())).thenReturn(
            CompletableFuture.completedFuture(deleteResponse)
        );

        DeleteDataObjectResponse response = sdkClient.deleteDataObjectAsync(deleteRequest, testThreadPool.executor(TEST_THREAD_POOL))
            .toCompletableFuture()
            .join();

        assertEquals(TEST_INDEX, deleteRequestCaptor.getValue().index());
        assertEquals(TEST_ID, response.id());

        org.opensearch.action.delete.DeleteResponse deleteActionResponse = org.opensearch.action.delete.DeleteResponse.fromXContent(
            response.parser()
        );
        assertEquals(TEST_ID, deleteActionResponse.getId());
        assertEquals(DocWriteResponse.Result.DELETED, deleteActionResponse.getResult());
        assertEquals(0, deleteActionResponse.getShardInfo().getFailed());
        assertEquals(2, deleteActionResponse.getShardInfo().getSuccessful());
        assertEquals(2, deleteActionResponse.getShardInfo().getTotal());
    }

    @Test
    public void testDeleteDataObject_NotFound() throws IOException {
        DeleteDataObjectRequest deleteRequest = DeleteDataObjectRequest.builder()
            .index(TEST_INDEX)
            .id(TEST_ID)
            .tenantId(TEST_TENANT_ID)
            .build();

        DeleteResponse deleteResponse = new DeleteResponse.Builder().id(TEST_ID)
            .index(TEST_INDEX)
            .primaryTerm(0)
            .result(Result.NotFound)
            .seqNo(0)
            .shards(new ShardStatistics.Builder().failed(0).successful(2).total(2).build())
            .version(0)
            .build();

        ArgumentCaptor<DeleteRequest> deleteRequestCaptor = ArgumentCaptor.forClass(DeleteRequest.class);
        when(mockedOpenSearchAsyncClient.delete(deleteRequestCaptor.capture())).thenReturn(
            CompletableFuture.completedFuture(deleteResponse)
        );

        DeleteDataObjectResponse response = sdkClient.deleteDataObjectAsync(deleteRequest, testThreadPool.executor(TEST_THREAD_POOL))
            .toCompletableFuture()
            .join();

        org.opensearch.action.delete.DeleteResponse deleteActionResponse = org.opensearch.action.delete.DeleteResponse.fromXContent(
            response.parser()
        );
        assertEquals(TEST_ID, deleteActionResponse.getId());
        assertEquals(DocWriteResponse.Result.NOT_FOUND, deleteActionResponse.getResult());
        assertEquals(0, deleteActionResponse.getShardInfo().getFailed());
        assertEquals(2, deleteActionResponse.getShardInfo().getSuccessful());
        assertEquals(2, deleteActionResponse.getShardInfo().getTotal());
    }

    @Test
    public void testDeleteDataObject_Exception() throws IOException {
        DeleteDataObjectRequest deleteRequest = DeleteDataObjectRequest.builder()
            .index(TEST_INDEX)
            .id(TEST_ID)
            .tenantId(TEST_TENANT_ID)
            .build();

        ArgumentCaptor<DeleteRequest> deleteRequestCaptor = ArgumentCaptor.forClass(DeleteRequest.class);
        when(mockedOpenSearchAsyncClient.delete(deleteRequestCaptor.capture())).thenReturn(
            CompletableFuture.failedFuture(new IOException("test"))
        );

        CompletableFuture<DeleteDataObjectResponse> future = sdkClient.deleteDataObjectAsync(
            deleteRequest,
            testThreadPool.executor(TEST_THREAD_POOL)
        ).toCompletableFuture();

        CompletionException ce = assertThrows(CompletionException.class, () -> future.join());
        assertEquals(OpenSearchStatusException.class, ce.getCause().getClass());
    }

    @Test
    public void testBulkDataObject() throws IOException {
        PutDataObjectRequest putRequest = PutDataObjectRequest.builder()
            .id(TEST_ID + "1")
            .tenantId(TEST_TENANT_ID)
            .dataObject(testDataObject)
            .build();
        UpdateDataObjectRequest updateRequest = UpdateDataObjectRequest.builder()
            .id(TEST_ID + "2")
            .tenantId(TEST_TENANT_ID)
            .dataObject(testDataObject)
            .build();
        DeleteDataObjectRequest deleteRequest = DeleteDataObjectRequest.builder().id(TEST_ID + "3").tenantId(TEST_TENANT_ID).build();

        BulkDataObjectRequest bulkRequest = BulkDataObjectRequest.builder()
            .globalIndex(TEST_INDEX)
            .build()
            .add(putRequest)
            .add(updateRequest)
            .add(deleteRequest);

        BulkResponse bulkResponse = new BulkResponse.Builder().took(100L)
            .items(
                Arrays.asList(
                    new BulkResponseItem.Builder().id(TEST_ID + "1")
                        .index(TEST_INDEX)
                        .operationType(OperationType.Index)
                        .result(Result.Created.jsonValue())
                        .status(RestStatus.OK.getStatus())
                        .build(),
                    new BulkResponseItem.Builder().id(TEST_ID + "2")
                        .index(TEST_INDEX)
                        .operationType(OperationType.Update)
                        .result(Result.Updated.jsonValue())
                        .status(RestStatus.OK.getStatus())
                        .build(),
                    new BulkResponseItem.Builder().id(TEST_ID + "3")
                        .index(TEST_INDEX)
                        .operationType(OperationType.Delete)
                        .result(Result.Deleted.jsonValue())
                        .status(RestStatus.OK.getStatus())
                        .build()
                )
            )
            .errors(false)
            .build();

        ArgumentCaptor<BulkRequest> bulkRequestCaptor = ArgumentCaptor.forClass(BulkRequest.class);
        when(mockedOpenSearchAsyncClient.bulk(bulkRequestCaptor.capture())).thenReturn(CompletableFuture.completedFuture(bulkResponse));

        BulkDataObjectResponse response = sdkClient.bulkDataObjectAsync(bulkRequest, testThreadPool.executor(TEST_THREAD_POOL))
            .toCompletableFuture()
            .join();

        assertEquals(3, bulkRequestCaptor.getValue().operations().size());
        assertEquals(3, response.getResponses().length);
        assertEquals(100L, response.getTookInMillis());

        assertTrue(response.getResponses()[0] instanceof PutDataObjectResponse);
        assertTrue(response.getResponses()[1] instanceof UpdateDataObjectResponse);
        assertTrue(response.getResponses()[2] instanceof DeleteDataObjectResponse);

        assertEquals(TEST_ID + "1", response.getResponses()[0].id());
        assertEquals(TEST_ID + "2", response.getResponses()[1].id());
        assertEquals(TEST_ID + "3", response.getResponses()[2].id());
    }

    @Test
    public void testBulkDataObject_WithFailures() throws IOException {
        PutDataObjectRequest putRequest = PutDataObjectRequest.builder()
            .id(TEST_ID + "1")
            .tenantId(TEST_TENANT_ID)
            .dataObject(testDataObject)
            .build();
        UpdateDataObjectRequest updateRequest = UpdateDataObjectRequest.builder()
            .id(TEST_ID + "2")
            .tenantId(TEST_TENANT_ID)
            .dataObject(testDataObject)
            .build();
        DeleteDataObjectRequest deleteRequest = DeleteDataObjectRequest.builder().id(TEST_ID + "3").tenantId(TEST_TENANT_ID).build();

        BulkDataObjectRequest bulkRequest = BulkDataObjectRequest.builder()
            .globalIndex(TEST_INDEX)
            .build()
            .add(putRequest)
            .add(updateRequest)
            .add(deleteRequest);

        BulkResponse bulkResponse = new BulkResponse.Builder().took(100L)
            .items(
                Arrays.asList(
                    new BulkResponseItem.Builder().id(TEST_ID + "1")
                        .index(TEST_INDEX)
                        .operationType(OperationType.Index)
                        .result(Result.Created.jsonValue())
                        .status(RestStatus.OK.getStatus())
                        .build(),
                    new BulkResponseItem.Builder().id(TEST_ID + "2")
                        .index(TEST_INDEX)
                        .operationType(OperationType.Update)
                        .error(new ErrorCause.Builder().type("update_error").reason("Update failed").build())
                        .status(RestStatus.INTERNAL_SERVER_ERROR.getStatus())
                        .build(),
                    new BulkResponseItem.Builder().id(TEST_ID + "3")
                        .index(TEST_INDEX)
                        .operationType(OperationType.Delete)
                        .result(Result.Deleted.jsonValue())
                        .status(RestStatus.OK.getStatus())
                        .build()
                )
            )
            .errors(true)
            .build();

        when(mockedOpenSearchAsyncClient.bulk(any(BulkRequest.class))).thenReturn(CompletableFuture.completedFuture(bulkResponse));

        BulkDataObjectResponse response = sdkClient.bulkDataObjectAsync(bulkRequest, testThreadPool.executor(TEST_THREAD_POOL))
            .toCompletableFuture()
            .join();

        assertEquals(3, response.getResponses().length);
        assertFalse(response.getResponses()[0].isFailed());
        assertTrue(response.getResponses()[0] instanceof PutDataObjectResponse);
        assertTrue(response.getResponses()[1].isFailed());
        assertTrue(response.getResponses()[1] instanceof UpdateDataObjectResponse);
        assertFalse(response.getResponses()[2].isFailed());
        assertTrue(response.getResponses()[2] instanceof DeleteDataObjectResponse);
    }

    @Test
    public void testBulkDataObject_Exception() throws OpenSearchException, IOException {
        PutDataObjectRequest putRequest = PutDataObjectRequest.builder()
            .index(TEST_INDEX)
            .id(TEST_ID)
            .tenantId(TEST_TENANT_ID)
            .dataObject(testDataObject)
            .build();

        BulkDataObjectRequest bulkRequest = BulkDataObjectRequest.builder().build().add(putRequest);

        when(mockedOpenSearchAsyncClient.bulk(any(BulkRequest.class))).thenReturn(
            CompletableFuture.failedFuture(
                new OpenSearchException(
                    new ErrorResponse.Builder().error(
                        new ErrorCause.Builder().type("parse_exception").reason("Failed to parse data object in a bulk response").build()
                    ).status(RestStatus.INTERNAL_SERVER_ERROR.getStatus()).build()
                )
            )
        );

        CompletableFuture<BulkDataObjectResponse> future = sdkClient.bulkDataObjectAsync(
            bulkRequest,
            testThreadPool.executor(TEST_THREAD_POOL)
        ).toCompletableFuture();

        CompletionException ce = assertThrows(CompletionException.class, () -> future.join());
        Throwable cause = ce.getCause();
        assertEquals(OpenSearchStatusException.class, cause.getClass());
        assertEquals(RestStatus.INTERNAL_SERVER_ERROR, ((OpenSearchStatusException) cause).status());
        ce = (CompletionException) cause.getCause();
        cause = ce.getCause();
        assertEquals(OpenSearchException.class, cause.getClass());
        assertEquals(RestStatus.INTERNAL_SERVER_ERROR.getStatus(), ((OpenSearchException) cause).status());
        assertTrue(cause.getMessage().contains("Failed to parse data object in a bulk response"));
    }

    @Test
    @SuppressWarnings({ "unchecked", "rawtypes" })
    public void testSearchDataObject() throws IOException {
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        SearchDataObjectRequest searchRequest = SearchDataObjectRequest.builder()
            .indices(TEST_INDEX)
            .tenantId(TEST_TENANT_ID)
            .searchSourceBuilder(searchSourceBuilder)
            .build();

        TotalHits totalHits = new TotalHits.Builder().value(0).relation(TotalHitsRelation.Eq).build();
        HitsMetadata<Object> hits = new HitsMetadata.Builder<>().hits(Collections.emptyList()).total(totalHits).build();
        ShardStatistics shards = new ShardStatistics.Builder().failed(0).successful(1).total(1).build();
        SearchResponse<?> searchResponse = new SearchResponse.Builder<>().hits(hits).took(1).timedOut(false).shards(shards).build();

        ArgumentCaptor<SearchRequest> getRequestCaptor = ArgumentCaptor.forClass(SearchRequest.class);
        ArgumentCaptor<Class<Map>> mapClassCaptor = ArgumentCaptor.forClass(Class.class);
        when(mockedOpenSearchAsyncClient.search(getRequestCaptor.capture(), mapClassCaptor.capture())).thenReturn(
            CompletableFuture.completedFuture((SearchResponse<Map>) searchResponse)
        );

        SearchDataObjectResponse response = sdkClient.searchDataObjectAsync(searchRequest, testThreadPool.executor(TEST_THREAD_POOL))
            .toCompletableFuture()
            .join();

        ArgumentCaptor<SearchRequest> requestCaptor = ArgumentCaptor.forClass(SearchRequest.class);
        verify(mockedOpenSearchAsyncClient, times(1)).search(requestCaptor.capture(), any());
        assertEquals(1, requestCaptor.getValue().index().size());
        assertEquals(TEST_INDEX, requestCaptor.getValue().index().get(0));
        Query query = requestCaptor.getValue().query();
        assertTrue(query.isBool());
        assertEquals(1, query.bool().must().size());
        assertEquals(1, query.bool().filter().size());
        assertTrue(query.bool().filter().get(0).isTerm());
        assertEquals(TENANT_ID_FIELD_KEY, query.bool().filter().get(0).term().field());
        assertEquals(TEST_TENANT_ID, query.bool().filter().get(0).term().value().stringValue());

        org.opensearch.action.search.SearchResponse searchActionResponse = org.opensearch.action.search.SearchResponse.fromXContent(
            response.parser()
        );
        assertEquals(TimeValue.timeValueMillis(1), searchActionResponse.getTook());
        assertFalse(searchActionResponse.isTimedOut());
        assertEquals(0, searchActionResponse.getFailedShards());
        assertEquals(1, searchActionResponse.getSuccessfulShards());
        assertEquals(1, searchActionResponse.getTotalShards());
    }

    @Test
    public void testSearchDataObject_Exception() throws IOException {
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        SearchDataObjectRequest searchRequest = SearchDataObjectRequest.builder()
            .indices(TEST_INDEX)
            .tenantId(TEST_TENANT_ID)
            .searchSourceBuilder(searchSourceBuilder)
            .build();

        when(mockedOpenSearchAsyncClient.search(any(SearchRequest.class), any())).thenReturn(
            CompletableFuture.failedFuture(new UnsupportedOperationException("test"))
        );
        CompletableFuture<SearchDataObjectResponse> future = sdkClient.searchDataObjectAsync(
            searchRequest,
            testThreadPool.executor(TEST_THREAD_POOL)
        ).toCompletableFuture();

        CompletionException ce = assertThrows(CompletionException.class, () -> future.join());
        Throwable cause = ce.getCause();
        assertEquals(OpenSearchStatusException.class, cause.getClass());
        assertEquals(RestStatus.INTERNAL_SERVER_ERROR, ((OpenSearchStatusException) cause).status());
        ce = (CompletionException) cause.getCause();
        cause = ce.getCause();
        assertEquals(UnsupportedOperationException.class, cause.getClass());
        assertEquals("test", cause.getMessage());
    }

    @Test
    public void testSearchDataObject_NullTenantNoMultitenancy() throws IOException {
        // Tests no status exception if multitenancy not enabled
        SdkClient sdkClientNoTenant = SdkClientFactory.wrapSdkClientDelegate(
            new RemoteClusterIndicesClient(mockedOpenSearchAsyncClient, null),
            false
        );

        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        SearchDataObjectRequest searchRequest = SearchDataObjectRequest.builder()
            .indices(TEST_INDEX)
            // null tenant Id
            .searchSourceBuilder(searchSourceBuilder)
            .build();

        when(mockedOpenSearchAsyncClient.search(any(SearchRequest.class), any())).thenReturn(
            CompletableFuture.failedFuture(new UnsupportedOperationException("test"))
        );
        CompletableFuture<SearchDataObjectResponse> future = sdkClientNoTenant.searchDataObjectAsync(
            searchRequest,
            testThreadPool.executor(TEST_THREAD_POOL)
        ).toCompletableFuture();

        CompletionException ce = assertThrows(CompletionException.class, () -> future.join());
        Throwable cause = ce.getCause();
        assertEquals(OpenSearchStatusException.class, cause.getClass());
        assertEquals(RestStatus.INTERNAL_SERVER_ERROR, ((OpenSearchStatusException) cause).status());
        ce = (CompletionException) cause.getCause();
        cause = ce.getCause();
        assertEquals(UnsupportedOperationException.class, cause.getClass());
        assertEquals("test", cause.getMessage());
    }

    @Test
    public void testParsingAggregations() throws IOException {
        String searchResponseJson = "{\n"
            + "  \"took\": 5,\n"
            + "  \"timed_out\": false,\n"
            + "  \"_shards\": {\n"
            + "    \"total\": 1,\n"
            + "    \"successful\": 1,\n"
            + "    \"skipped\": 0,\n"
            + "    \"failed\": 0\n"
            + "  },\n"
            + "  \"hits\": {\n"
            + "    \"total\": {\n"
            + "      \"value\": 0,\n"
            + "      \"relation\": \"eq\"\n"
            + "    },\n"
            + "    \"max_score\": null,\n"
            + "    \"hits\": []\n"
            + "  },\n"
            + "  \"aggregations\": {\n"
            + "    \"sterms#unique2_connector_names\": {\n"
            + "      \"doc_count_error_upper_bound\": 0,\n"
            + "      \"sum_other_doc_count\": 0,\n"
            + "      \"buckets\": [\n"
            + "        {\n"
            + "          \"key\": \"sample_connector\",\n"
            + "          \"doc_count\": 5\n"
            + "        }\n"
            + "      ]\n"
            + "    }\n"
            + "  }\n"
            + "}";
        XContentParser parser = ((RemoteClusterIndicesClient) sdkClient.getDelegate()).createParser(searchResponseJson);
        org.opensearch.action.search.SearchResponse response = org.opensearch.action.search.SearchResponse.fromXContent(parser);
        assertTrue(response.getAggregations().asMap().containsKey("unique2_connector_names"));
    }
}
