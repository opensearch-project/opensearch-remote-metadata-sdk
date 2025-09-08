/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.remote.metadata.client.impl;

import org.opensearch.OpenSearchStatusException;
import org.opensearch.action.DocWriteRequest.OpType;
import org.opensearch.action.DocWriteResponse;
import org.opensearch.action.DocWriteResponse.Result;
import org.opensearch.action.bulk.BulkItemResponse;
import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.action.delete.DeleteRequest;
import org.opensearch.action.delete.DeleteResponse;
import org.opensearch.action.get.GetRequest;
import org.opensearch.action.get.GetResponse;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.action.search.SearchPhaseName;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.search.ShardSearchFailure;
import org.opensearch.action.support.replication.ReplicationResponse.ShardInfo;
import org.opensearch.action.update.UpdateRequest;
import org.opensearch.action.update.UpdateResponse;
import org.opensearch.common.xcontent.LoggingDeprecationHandler;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.index.IndexNotFoundException;
import org.opensearch.index.engine.VersionConflictEngineException;
import org.opensearch.index.get.GetResult;
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
import org.opensearch.search.internal.InternalSearchResponse;
import org.opensearch.transport.RemoteTransportException;
import org.opensearch.transport.client.Client;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.EnumSet;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.stream.Collectors;

import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import static org.opensearch.core.xcontent.XContentParserUtils.ensureExpectedToken;
import static org.opensearch.remote.metadata.common.CommonValue.TENANT_ID_FIELD_KEY;
import static org.junit.Assert.assertNull;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class LocalClusterIndicesClientTests {

    private static final String TEST_ID = "123";
    private static final String TEST_INDEX = "test_index";
    private static final String TEST_TENANT_ID = "xyz";
    private static final String TENANT_ID_FIELD = "tenant_id";
    private static final String GLOBAL_TENANT_ID = "test_global_tenant";
    private LocalClusterIndicesClient innerClient;

    @Mock
    private Client mockedClient;
    private SdkClient sdkClient;

    @Mock
    private NamedXContentRegistry xContentRegistry;

    private TestDataObject testDataObject;

    @BeforeEach
    public void setup() {
        MockitoAnnotations.openMocks(this);

        innerClient = new LocalClusterIndicesClient(mockedClient, xContentRegistry, Map.of(TENANT_ID_FIELD_KEY, TENANT_ID_FIELD));
        sdkClient = new SdkClient(innerClient, true, null);

        testDataObject = new TestDataObject("foo");
    }

    @Test
    public void testPutDataObject() throws IOException {
        PutDataObjectRequest putRequest = PutDataObjectRequest.builder()
            .index(TEST_INDEX)
            .id(TEST_ID)
            .tenantId(TEST_TENANT_ID)
            .overwriteIfExists(false)
            .dataObject(testDataObject)
            .build();

        IndexResponse indexResponse = new IndexResponse(new ShardId(TEST_INDEX, "_na_", 0), TEST_ID, 1, 0, 2, true);
        doAnswer(invocation -> {
            ActionListener<IndexResponse> listener = invocation.getArgument(1);
            listener.onResponse(indexResponse);
            return null;
        }).when(mockedClient).index(any(IndexRequest.class), any());

        PutDataObjectResponse response = sdkClient.putDataObjectAsync(putRequest).toCompletableFuture().join();

        ArgumentCaptor<IndexRequest> requestCaptor = ArgumentCaptor.forClass(IndexRequest.class);
        verify(mockedClient, times(1)).index(requestCaptor.capture(), any());
        assertEquals(TEST_INDEX, requestCaptor.getValue().index());
        assertEquals(TEST_ID, requestCaptor.getValue().id());
        assertEquals(OpType.CREATE, requestCaptor.getValue().opType());

        assertEquals(TEST_ID, response.id());

        IndexResponse indexActionResponse = IndexResponse.fromXContent(response.parser());
        assertEquals(TEST_ID, indexActionResponse.getId());
        assertEquals(DocWriteResponse.Result.CREATED, indexActionResponse.getResult());

        // Test null id
        putRequest = PutDataObjectRequest.builder()
            .index(TEST_INDEX)
            .tenantId(TEST_TENANT_ID)
            .overwriteIfExists(false)
            .dataObject(testDataObject)
            .build();

        response = sdkClient.putDataObjectAsync(putRequest).toCompletableFuture().join();

        requestCaptor = ArgumentCaptor.forClass(IndexRequest.class);
        verify(mockedClient, times(2)).index(requestCaptor.capture(), any());
        assertEquals(TEST_INDEX, requestCaptor.getValue().index());
        assertNull(TEST_ID, requestCaptor.getValue().id());
        assertEquals(OpType.CREATE, requestCaptor.getValue().opType());

        // Test empty id
        final PutDataObjectRequest putRequestEmptyId = PutDataObjectRequest.builder()
            .index(TEST_INDEX)
            .id("")
            .tenantId(TEST_TENANT_ID)
            .overwriteIfExists(false)
            .dataObject(testDataObject)
            .build();

        IllegalArgumentException ex = assertThrows(
            IllegalArgumentException.class,
            () -> sdkClient.putDataObjectAsync(putRequestEmptyId).toCompletableFuture().join()
        );
        assertEquals("if _id is specified it must not be empty", ex.getMessage());
    }

    @Test
    public void testPutDataObject_Exception() throws IOException {
        PutDataObjectRequest putRequest = PutDataObjectRequest.builder()
            .index(TEST_INDEX)
            .tenantId(TEST_TENANT_ID)
            .dataObject(testDataObject)
            .build();

        doAnswer(invocation -> {
            ActionListener<IndexResponse> listener = invocation.getArgument(1);
            listener.onFailure(new UnsupportedOperationException("test"));
            return null;
        }).when(mockedClient).index(any(IndexRequest.class), any());

        CompletableFuture<PutDataObjectResponse> future = sdkClient.putDataObjectAsync(putRequest).toCompletableFuture();

        CompletionException ce = assertThrows(CompletionException.class, () -> future.join());
        Throwable cause = ce.getCause();
        assertEquals(OpenSearchStatusException.class, cause.getClass());
        assertEquals("Failed to put data object in index test_index", cause.getMessage());
    }

    @Test
    public void testPutDataObject_IOException() throws IOException {
        ToXContentObject badDataObject = new ToXContentObject() {
            @Override
            public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
                throw new IOException("test");
            }
        };
        PutDataObjectRequest putRequest = PutDataObjectRequest.builder()
            .index(TEST_INDEX)
            .tenantId(TEST_TENANT_ID)
            .dataObject(badDataObject)
            .build();

        CompletableFuture<PutDataObjectResponse> future = sdkClient.putDataObjectAsync(putRequest).toCompletableFuture();

        CompletionException ce = assertThrows(CompletionException.class, () -> future.join());
        Throwable cause = ce.getCause();
        assertEquals(OpenSearchStatusException.class, cause.getClass());
        assertEquals(RestStatus.BAD_REQUEST, ((OpenSearchStatusException) cause).status());
    }

    @Test
    public void testPutDataObject_VersionCheck() throws IOException {
        PutDataObjectRequest putRequest = PutDataObjectRequest.builder()
            .index(TEST_INDEX)
            .id(TEST_ID)
            .tenantId(TEST_TENANT_ID)
            .dataObject(testDataObject)
            .ifSeqNo(5L)
            .ifPrimaryTerm(2L)
            .build();

        IndexResponse indexResponse = new IndexResponse(new ShardId(TEST_INDEX, "_na_", 0), TEST_ID, 1, 0, 2, true);
        doAnswer(invocation -> {
            ActionListener<IndexResponse> listener = invocation.getArgument(1);
            listener.onResponse(indexResponse);
            return null;
        }).when(mockedClient).index(any(IndexRequest.class), any());

        PutDataObjectResponse response = sdkClient.putDataObjectAsync(putRequest).toCompletableFuture().join();

        ArgumentCaptor<IndexRequest> requestCaptor = ArgumentCaptor.forClass(IndexRequest.class);
        verify(mockedClient, times(1)).index(requestCaptor.capture(), any());
        assertEquals(TEST_INDEX, requestCaptor.getValue().index());
        assertEquals(TEST_ID, requestCaptor.getValue().id());
        assertEquals(5L, requestCaptor.getValue().ifSeqNo());
        assertEquals(2L, requestCaptor.getValue().ifPrimaryTerm());
    }

    @Test
    public void testPutDataObject_VersionConflict() throws IOException {
        PutDataObjectRequest putRequest = PutDataObjectRequest.builder()
            .index(TEST_INDEX)
            .id(TEST_ID)
            .tenantId(TEST_TENANT_ID)
            .dataObject(testDataObject)
            .ifSeqNo(5L)
            .ifPrimaryTerm(2L)
            .build();

        doAnswer(invocation -> {
            ActionListener<IndexResponse> listener = invocation.getArgument(1);
            listener.onFailure(new VersionConflictEngineException(new ShardId(TEST_INDEX, "_na_", 0), TEST_ID, "version conflict"));
            return null;
        }).when(mockedClient).index(any(IndexRequest.class), any());

        CompletableFuture<PutDataObjectResponse> future = sdkClient.putDataObjectAsync(putRequest).toCompletableFuture();

        CompletionException ce = assertThrows(CompletionException.class, () -> future.join());
        Throwable cause = ce.getCause();
        assertEquals(OpenSearchStatusException.class, cause.getClass());
        assertEquals(RestStatus.CONFLICT, ((OpenSearchStatusException) cause).status());
    }

    @Test
    public void testGetDataObject() throws IOException {
        GetDataObjectRequest getRequest = GetDataObjectRequest.builder().index(TEST_INDEX).id(TEST_ID).tenantId(TEST_TENANT_ID).build();

        String json = testDataObject.toJson();
        GetResponse getResponse = new GetResponse(new GetResult(TEST_INDEX, TEST_ID, -2, 0, 1, true, new BytesArray(json), null, null));

        doAnswer(invocation -> {
            ActionListener<GetResponse> listener = invocation.getArgument(1);
            listener.onResponse(getResponse);
            return null;
        }).when(mockedClient).get(any(GetRequest.class), any());

        GetDataObjectResponse response = sdkClient.getDataObjectAsync(getRequest).toCompletableFuture().join();

        ArgumentCaptor<GetRequest> requestCaptor = ArgumentCaptor.forClass(GetRequest.class);
        verify(mockedClient, times(1)).get(requestCaptor.capture(), any());
        assertEquals(TEST_INDEX, requestCaptor.getValue().index());
        assertEquals(TEST_ID, response.id());
        assertEquals("foo", response.source().get("data"));
        XContentParser parser = response.parser();
        XContentParser dataParser = XContentHelper.createParser(
            NamedXContentRegistry.EMPTY,
            LoggingDeprecationHandler.INSTANCE,
            GetResponse.fromXContent(parser).getSourceAsBytesRef(),
            XContentType.JSON
        );
        ensureExpectedToken(XContentParser.Token.START_OBJECT, dataParser.nextToken(), dataParser);
        assertEquals("foo", TestDataObject.parse(dataParser).data());
    }

    @Test
    public void testGetDataObject_NotFound() throws IOException {
        GetDataObjectRequest getRequest = GetDataObjectRequest.builder().index(TEST_INDEX).id(TEST_ID).tenantId(TEST_TENANT_ID).build();
        GetResponse getResponse = new GetResponse(new GetResult(TEST_INDEX, TEST_ID, -2, 0, 1, false, null, null, null));

        doAnswer(invocation -> {
            ActionListener<GetResponse> listener = invocation.getArgument(1);
            listener.onResponse(getResponse);
            return null;
        }).when(mockedClient).get(any(GetRequest.class), any());

        GetDataObjectResponse response = sdkClient.getDataObjectAsync(getRequest).toCompletableFuture().join();

        ArgumentCaptor<GetRequest> requestCaptor = ArgumentCaptor.forClass(GetRequest.class);
        verify(mockedClient, times(1)).get(requestCaptor.capture(), any());
        assertEquals(TEST_INDEX, requestCaptor.getValue().index());
        assertEquals(TEST_ID, response.id());
        assertTrue(response.source().isEmpty());
    }

    @Test
    public void testGetDataObject_Exception() throws IOException {
        GetDataObjectRequest getRequest = GetDataObjectRequest.builder().index(TEST_INDEX).id(TEST_ID).tenantId(TEST_TENANT_ID).build();

        doAnswer(invocation -> {
            ActionListener<GetResponse> listener = invocation.getArgument(1);
            listener.onFailure(new UnsupportedOperationException("test"));
            return null;
        }).when(mockedClient).get(any(GetRequest.class), any());

        CompletableFuture<GetDataObjectResponse> future = sdkClient.getDataObjectAsync(getRequest).toCompletableFuture();

        CompletionException ce = assertThrows(CompletionException.class, () -> future.join());
        Throwable cause = ce.getCause();
        assertEquals(OpenSearchStatusException.class, cause.getClass());
        assertEquals("Failed to get data object from index test_index", cause.getMessage());
    }

    @Test
    public void testUpdateDataObject() throws IOException {
        UpdateDataObjectRequest updateRequest = UpdateDataObjectRequest.builder()
            .index(TEST_INDEX)
            .id(TEST_ID)
            .tenantId(TEST_TENANT_ID)
            .retryOnConflict(3)
            .dataObject(testDataObject)
            .build();

        UpdateResponse updateResponse = new UpdateResponse(
            new ShardInfo(1, 1),
            new ShardId(TEST_INDEX, "_na_", 0),
            TEST_ID,
            1,
            0,
            2,
            Result.UPDATED
        );
        doAnswer(invocation -> {
            ActionListener<UpdateResponse> listener = invocation.getArgument(1);
            listener.onResponse(updateResponse);
            return null;
        }).when(mockedClient).update(any(UpdateRequest.class), any());

        UpdateDataObjectResponse response = sdkClient.updateDataObjectAsync(updateRequest).toCompletableFuture().join();

        ArgumentCaptor<UpdateRequest> requestCaptor = ArgumentCaptor.forClass(UpdateRequest.class);
        verify(mockedClient, times(1)).update(requestCaptor.capture(), any());
        assertEquals(TEST_INDEX, requestCaptor.getValue().index());
        assertEquals(3, requestCaptor.getValue().retryOnConflict());
        assertEquals(TEST_ID, response.id());

        UpdateResponse updateActionResponse = UpdateResponse.fromXContent(response.parser());
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
            .dataObject(Map.of("foo", "bar"))
            .build();

        UpdateResponse updateResponse = new UpdateResponse(
            new ShardInfo(1, 1),
            new ShardId(TEST_INDEX, "_na_", 0),
            TEST_ID,
            1,
            0,
            2,
            Result.UPDATED
        );

        doAnswer(invocation -> {
            ActionListener<UpdateResponse> listener = invocation.getArgument(1);
            listener.onResponse(updateResponse);
            return null;
        }).when(mockedClient).update(any(UpdateRequest.class), any());

        UpdateDataObjectResponse response = sdkClient.updateDataObjectAsync(updateRequest).toCompletableFuture().join();

        ArgumentCaptor<UpdateRequest> requestCaptor = ArgumentCaptor.forClass(UpdateRequest.class);
        verify(mockedClient, times(1)).update(requestCaptor.capture(), any());
        assertEquals(TEST_INDEX, requestCaptor.getValue().index());
        assertEquals(TEST_ID, requestCaptor.getValue().id());
        UpdateResponse updateActionResponse = UpdateResponse.fromXContent(response.parser());
        assertEquals(TEST_ID, updateActionResponse.getId());
        assertEquals(DocWriteResponse.Result.UPDATED, updateActionResponse.getResult());
        assertEquals("bar", requestCaptor.getValue().doc().sourceAsMap().get("foo"));
    }

    @Test
    public void testUpdateDataObject_NotFound() throws IOException {
        UpdateDataObjectRequest updateRequest = UpdateDataObjectRequest.builder()
            .index(TEST_INDEX)
            .id(TEST_ID)
            .tenantId(TEST_TENANT_ID)
            .dataObject(testDataObject)
            .build();

        UpdateResponse updateResponse = new UpdateResponse(
            new ShardInfo(1, 1),
            new ShardId(TEST_INDEX, "_na_", 0),
            TEST_ID,
            1,
            0,
            2,
            Result.CREATED
        );
        doAnswer(invocation -> {
            ActionListener<UpdateResponse> listener = invocation.getArgument(1);
            listener.onResponse(updateResponse);
            return null;
        }).when(mockedClient).update(any(UpdateRequest.class), any());

        UpdateDataObjectResponse response = sdkClient.updateDataObjectAsync(updateRequest).toCompletableFuture().join();

        ArgumentCaptor<UpdateRequest> requestCaptor = ArgumentCaptor.forClass(UpdateRequest.class);
        verify(mockedClient, times(1)).update(requestCaptor.capture(), any());
        assertEquals(TEST_INDEX, requestCaptor.getValue().index());
        assertEquals(TEST_ID, response.id());

        UpdateResponse updateActionResponse = UpdateResponse.fromXContent(response.parser());
        assertEquals(TEST_ID, updateActionResponse.getId());
        assertEquals(DocWriteResponse.Result.CREATED, updateActionResponse.getResult());
        assertEquals(0, updateActionResponse.getShardInfo().getFailed());
        assertEquals(1, updateActionResponse.getShardInfo().getSuccessful());
        assertEquals(1, updateActionResponse.getShardInfo().getTotal());
    }

    @Test
    public void testUpdateDataObject_Exception() throws IOException {
        UpdateDataObjectRequest updateRequest = UpdateDataObjectRequest.builder()
            .index(TEST_INDEX)
            .id(TEST_ID)
            .tenantId(TEST_TENANT_ID)
            .dataObject(testDataObject)
            .build();

        doAnswer(invocation -> {
            ActionListener<UpdateResponse> listener = invocation.getArgument(1);
            listener.onFailure(new UnsupportedOperationException("test"));
            return null;
        }).when(mockedClient).update(any(UpdateRequest.class), any());

        CompletableFuture<UpdateDataObjectResponse> future = sdkClient.updateDataObjectAsync(updateRequest).toCompletableFuture();

        CompletionException ce = assertThrows(CompletionException.class, () -> future.join());
        Throwable cause = ce.getCause();
        assertEquals(OpenSearchStatusException.class, cause.getClass());
        assertEquals("Failed to update data object in index test_index", cause.getMessage());
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

        doAnswer(invocation -> {
            ActionListener<UpdateResponse> listener = invocation.getArgument(1);
            listener.onFailure(new VersionConflictEngineException(new ShardId(TEST_INDEX, "_na_", 0), TEST_ID, "test"));
            return null;
        }).when(mockedClient).update(any(UpdateRequest.class), any());

        CompletableFuture<UpdateDataObjectResponse> future = sdkClient.updateDataObjectAsync(updateRequest).toCompletableFuture();

        CompletionException ce = assertThrows(CompletionException.class, () -> future.join());
        Throwable cause = ce.getCause();
        assertEquals(OpenSearchStatusException.class, cause.getClass());
        assertEquals(RestStatus.CONFLICT, ((OpenSearchStatusException) cause).status());
    }

    @Test
    public void testUpdateDataObject_VersionConflict() throws IOException {
        UpdateDataObjectRequest updateRequest = UpdateDataObjectRequest.builder()
            .index(TEST_INDEX)
            .id(TEST_ID)
            .tenantId(TEST_TENANT_ID)
            .dataObject(testDataObject)
            .ifSeqNo(5)
            .ifPrimaryTerm(2)
            .build();

        doAnswer(invocation -> {
            ActionListener<UpdateResponse> listener = invocation.getArgument(1);
            RemoteTransportException rte = Mockito.mock(RemoteTransportException.class);
            when(rte.getCause()).thenReturn(new VersionConflictEngineException(new ShardId(TEST_INDEX, "_na_", 0), TEST_ID, "test"));
            listener.onFailure(rte);
            return null;
        }).when(mockedClient).update(any(UpdateRequest.class), any());

        CompletableFuture<UpdateDataObjectResponse> future = sdkClient.updateDataObjectAsync(updateRequest).toCompletableFuture();

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

        DeleteResponse deleteResponse = new DeleteResponse(new ShardId(TEST_INDEX, "_na_", 0), TEST_ID, 1, 0, 2, true);

        doAnswer(invocation -> {
            ActionListener<DeleteResponse> listener = invocation.getArgument(1);
            listener.onResponse(deleteResponse);
            return null;
        }).when(mockedClient).delete(any(DeleteRequest.class), any());

        DeleteDataObjectResponse response = sdkClient.deleteDataObjectAsync(deleteRequest).toCompletableFuture().join();

        ArgumentCaptor<DeleteRequest> requestCaptor = ArgumentCaptor.forClass(DeleteRequest.class);
        verify(mockedClient, times(1)).delete(requestCaptor.capture(), any());
        assertEquals(TEST_INDEX, requestCaptor.getValue().index());
        assertEquals(TEST_ID, response.id());

        DeleteResponse deleteActionResponse = DeleteResponse.fromXContent(response.parser());
        assertEquals(TEST_ID, deleteActionResponse.getId());
        assertEquals(DocWriteResponse.Result.DELETED, deleteActionResponse.getResult());
    }

    @Test
    public void testDeleteDataObject_Exception() throws IOException {
        DeleteDataObjectRequest deleteRequest = DeleteDataObjectRequest.builder()
            .index(TEST_INDEX)
            .id(TEST_ID)
            .tenantId(TEST_TENANT_ID)
            .build();

        doAnswer(invocation -> {
            ActionListener<DeleteResponse> listener = invocation.getArgument(1);
            listener.onFailure(new UnsupportedOperationException("test"));
            return null;
        }).when(mockedClient).delete(any(DeleteRequest.class), any());

        CompletableFuture<DeleteDataObjectResponse> future = sdkClient.deleteDataObjectAsync(deleteRequest).toCompletableFuture();

        CompletionException ce = assertThrows(CompletionException.class, () -> future.join());
        Throwable cause = ce.getCause();
        assertEquals(OpenSearchStatusException.class, cause.getClass());
        assertEquals("Failed to delete data object from index test_index", cause.getMessage());
    }

    @Test
    public void testDeleteDataObject_VersionCheck() throws IOException {
        DeleteDataObjectRequest deleteRequest = DeleteDataObjectRequest.builder()
            .index(TEST_INDEX)
            .id(TEST_ID)
            .tenantId(TEST_TENANT_ID)
            .ifSeqNo(5L)
            .ifPrimaryTerm(2L)
            .build();

        DeleteResponse deleteResponse = new DeleteResponse(new ShardId(TEST_INDEX, "_na_", 0), TEST_ID, 1, 0, 2, true);

        doAnswer(invocation -> {
            ActionListener<DeleteResponse> listener = invocation.getArgument(1);
            listener.onResponse(deleteResponse);
            return null;
        }).when(mockedClient).delete(any(DeleteRequest.class), any());

        DeleteDataObjectResponse response = sdkClient.deleteDataObjectAsync(deleteRequest).toCompletableFuture().join();

        ArgumentCaptor<DeleteRequest> requestCaptor = ArgumentCaptor.forClass(DeleteRequest.class);
        verify(mockedClient, times(1)).delete(requestCaptor.capture(), any());
        assertEquals(TEST_INDEX, requestCaptor.getValue().index());
        assertEquals(TEST_ID, requestCaptor.getValue().id());
        assertEquals(5L, requestCaptor.getValue().ifSeqNo());
        assertEquals(2L, requestCaptor.getValue().ifPrimaryTerm());
    }

    @Test
    public void testDeleteDataObject_VersionConflict() throws IOException {
        DeleteDataObjectRequest deleteRequest = DeleteDataObjectRequest.builder()
            .index(TEST_INDEX)
            .id(TEST_ID)
            .tenantId(TEST_TENANT_ID)
            .ifSeqNo(5L)
            .ifPrimaryTerm(2L)
            .build();

        doAnswer(invocation -> {
            ActionListener<DeleteResponse> listener = invocation.getArgument(1);
            listener.onFailure(new VersionConflictEngineException(new ShardId(TEST_INDEX, "_na_", 0), TEST_ID, "version conflict"));
            return null;
        }).when(mockedClient).delete(any(DeleteRequest.class), any());

        CompletableFuture<DeleteDataObjectResponse> future = sdkClient.deleteDataObjectAsync(deleteRequest).toCompletableFuture();

        CompletionException ce = assertThrows(CompletionException.class, () -> future.join());
        Throwable cause = ce.getCause();
        assertEquals(OpenSearchStatusException.class, cause.getClass());
        assertEquals(RestStatus.CONFLICT, ((OpenSearchStatusException) cause).status());
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

        ShardId shardId = new ShardId(TEST_INDEX, "_na_", 0);
        ShardInfo shardInfo = new ShardInfo(1, 1);

        IndexResponse indexResponse = new IndexResponse(shardId, TEST_ID + "1", 1, 1, 1, true);
        indexResponse.setShardInfo(shardInfo);

        UpdateResponse updateResponse = new UpdateResponse(shardId, TEST_ID + "2", 1, 1, 1, DocWriteResponse.Result.UPDATED);
        updateResponse.setShardInfo(shardInfo);

        DeleteResponse deleteResponse = new DeleteResponse(shardId, TEST_ID + "3", 1, 1, 1, true);
        deleteResponse.setShardInfo(shardInfo);

        BulkResponse bulkResponse = new BulkResponse(
            new BulkItemResponse[] {
                new BulkItemResponse(0, OpType.INDEX, indexResponse),
                new BulkItemResponse(1, OpType.UPDATE, updateResponse),
                new BulkItemResponse(2, OpType.DELETE, deleteResponse) },
            100L
        );

        doAnswer(invocation -> {
            ActionListener<BulkResponse> listener = invocation.getArgument(1);
            listener.onResponse(bulkResponse);
            return null;
        }).when(mockedClient).bulk(any(BulkRequest.class), any());

        BulkDataObjectResponse response = sdkClient.bulkDataObjectAsync(bulkRequest).toCompletableFuture().join();

        ArgumentCaptor<BulkRequest> requestCaptor = ArgumentCaptor.forClass(BulkRequest.class);
        verify(mockedClient, times(1)).bulk(requestCaptor.capture(), any());
        assertEquals(3, requestCaptor.getValue().numberOfActions());

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

        BulkResponse bulkResponse = new BulkResponse(
            new BulkItemResponse[] {
                new BulkItemResponse(0, OpType.INDEX, new IndexResponse(new ShardId(TEST_INDEX, "_na_", 0), TEST_ID + "1", 1, 1, 1, true)),
                new BulkItemResponse(
                    1,
                    OpType.UPDATE,
                    new BulkItemResponse.Failure(TEST_INDEX, TEST_ID + "2", new Exception("Update failed"))
                ),
                new BulkItemResponse(
                    0,
                    OpType.DELETE,
                    new DeleteResponse(new ShardId(TEST_INDEX, "_na_", 0), TEST_ID + "3", 1, 1, 1, true)
                ) },
            100L
        );

        doAnswer(invocation -> {
            ActionListener<BulkResponse> listener = invocation.getArgument(1);
            listener.onResponse(bulkResponse);
            return null;
        }).when(mockedClient).bulk(any(BulkRequest.class), any());

        BulkDataObjectResponse response = sdkClient.bulkDataObjectAsync(bulkRequest).toCompletableFuture().join();

        assertEquals(3, response.getResponses().length);
        assertFalse(response.getResponses()[0].isFailed());
        assertTrue(response.getResponses()[0] instanceof PutDataObjectResponse);
        assertTrue(response.getResponses()[1].isFailed());
        assertTrue(response.getResponses()[1] instanceof UpdateDataObjectResponse);
        assertFalse(response.getResponses()[2].isFailed());
        assertTrue(response.getResponses()[2] instanceof DeleteDataObjectResponse);
    }

    @Test
    public void testBulkDataObject_Exception() {
        PutDataObjectRequest putRequest = PutDataObjectRequest.builder()
            .index(TEST_INDEX)
            .id(TEST_ID)
            .tenantId(TEST_TENANT_ID)
            .dataObject(testDataObject)
            .build();

        BulkDataObjectRequest bulkRequest = BulkDataObjectRequest.builder().build().add(putRequest);

        doAnswer(invocation -> {
            ActionListener<BulkResponse> listener = invocation.getArgument(1);
            listener.onFailure(new OpenSearchStatusException("test", RestStatus.INTERNAL_SERVER_ERROR));
            return null;
        }).when(mockedClient).bulk(any(BulkRequest.class), any());

        CompletableFuture<BulkDataObjectResponse> future = sdkClient.bulkDataObjectAsync(bulkRequest).toCompletableFuture();

        CompletionException ce = assertThrows(CompletionException.class, () -> future.join());
        Throwable cause = ce.getCause();
        assertEquals(OpenSearchStatusException.class, cause.getClass());
        assertEquals(RestStatus.INTERNAL_SERVER_ERROR, ((OpenSearchStatusException) cause).status());
        assertEquals("Failed to execute bulk request", cause.getMessage());
    }

    @Test
    public void testSearchDataObjectNotTenantAware() throws IOException {
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        SearchDataObjectRequest searchRequest = SearchDataObjectRequest.builder()
            .indices(TEST_INDEX)
            .tenantId(TEST_TENANT_ID)
            .searchSourceBuilder(searchSourceBuilder)
            .build();

        SearchResponse searchResponse = new SearchResponse(
            InternalSearchResponse.empty(),
            null,
            1,
            1,
            0,
            123,
            new SearchResponse.PhaseTook(
                EnumSet.allOf(SearchPhaseName.class).stream().collect(Collectors.toMap(SearchPhaseName::getName, e -> (long) e.ordinal()))
            ),
            new ShardSearchFailure[0],
            SearchResponse.Clusters.EMPTY,
            null
        );

        doAnswer(invocation -> {
            ActionListener<SearchResponse> listener = invocation.getArgument(1);
            listener.onResponse(searchResponse);
            return null;
        }).when(mockedClient).search(any(SearchRequest.class), any());

        LocalClusterIndicesClient innerClient = new LocalClusterIndicesClient(
            mockedClient,
            xContentRegistry,
            Map.of(TENANT_ID_FIELD_KEY, TENANT_ID_FIELD)
        );
        SdkClient sdkClientNoTenant = new SdkClient(innerClient, false, null);
        SearchDataObjectResponse response = sdkClientNoTenant.searchDataObjectAsync(searchRequest).toCompletableFuture().join();

        ArgumentCaptor<SearchRequest> requestCaptor = ArgumentCaptor.forClass(SearchRequest.class);
        verify(mockedClient, times(1)).search(requestCaptor.capture(), any());
        assertEquals(1, requestCaptor.getValue().indices().length);
        assertEquals(TEST_INDEX, requestCaptor.getValue().indices()[0]);
        assertEquals("{}", requestCaptor.getValue().source().toString());

        SearchResponse searchActionResponse = SearchResponse.fromXContent(response.parser());
        assertEquals(0, searchActionResponse.getFailedShards());
        assertEquals(0, searchActionResponse.getSkippedShards());
        assertEquals(1, searchActionResponse.getSuccessfulShards());
        assertEquals(1, searchActionResponse.getTotalShards());
        assertEquals(0, searchActionResponse.getHits().getTotalHits().value());
    }

    @Test
    public void testSearchDataObjectTenantAware() throws IOException {
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        SearchDataObjectRequest searchRequest = SearchDataObjectRequest.builder()
            .indices(TEST_INDEX)
            .tenantId(TEST_TENANT_ID)
            .searchSourceBuilder(searchSourceBuilder)
            .build();

        SearchResponse searchResponse = new SearchResponse(
            InternalSearchResponse.empty(),
            null,
            1,
            1,
            0,
            123,
            new SearchResponse.PhaseTook(
                EnumSet.allOf(SearchPhaseName.class).stream().collect(Collectors.toMap(SearchPhaseName::getName, e -> (long) e.ordinal()))
            ),
            new ShardSearchFailure[0],
            SearchResponse.Clusters.EMPTY,
            null
        );

        doAnswer(invocation -> {
            ActionListener<SearchResponse> listener = invocation.getArgument(1);
            listener.onResponse(searchResponse);
            return null;
        }).when(mockedClient).search(any(SearchRequest.class), any());

        SearchDataObjectResponse response = sdkClient.searchDataObjectAsync(searchRequest).toCompletableFuture().join();

        ArgumentCaptor<SearchRequest> requestCaptor = ArgumentCaptor.forClass(SearchRequest.class);
        verify(mockedClient, times(1)).search(requestCaptor.capture(), any());
        assertEquals(1, requestCaptor.getValue().indices().length);
        assertEquals(TEST_INDEX, requestCaptor.getValue().indices()[0]);
        assertTrue(requestCaptor.getValue().source().toString().contains("{\"term\":{\"tenant_id\":{\"value\":\"xyz\""));

        SearchResponse searchActionResponse = SearchResponse.fromXContent(response.parser());
        assertEquals(0, searchActionResponse.getFailedShards());
        assertEquals(0, searchActionResponse.getSkippedShards());
        assertEquals(1, searchActionResponse.getSuccessfulShards());
        assertEquals(1, searchActionResponse.getTotalShards());
        assertEquals(0, searchActionResponse.getHits().getTotalHits().value());
    }

    @Test
    public void testSearchDataObject_IndexNotFoundException() throws IOException {
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        SearchDataObjectRequest searchRequest = SearchDataObjectRequest.builder()
            .indices(TEST_INDEX)
            .tenantId(TEST_TENANT_ID)
            .searchSourceBuilder(searchSourceBuilder)
            .build();

        doAnswer(invocation -> {
            ActionListener<SearchResponse> listener = invocation.getArgument(1);
            listener.onFailure(new IndexNotFoundException("test"));
            return null;
        }).when(mockedClient).search(any(SearchRequest.class), any());

        CompletableFuture<SearchDataObjectResponse> future = sdkClient.searchDataObjectAsync(searchRequest).toCompletableFuture();

        CompletionException ce = assertThrows(CompletionException.class, () -> future.join());
        Throwable cause = ce.getCause();
        assertEquals(OpenSearchStatusException.class, cause.getClass());
        assertEquals(RestStatus.NOT_FOUND, ((OpenSearchStatusException) cause).status());
        assertEquals("Failed to search indices [test_index]", cause.getMessage());
    }

    @Test
    public void testSearchDataObject_Exception() throws IOException {
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        SearchDataObjectRequest searchRequest = SearchDataObjectRequest.builder()
            .indices(TEST_INDEX)
            .tenantId(TEST_TENANT_ID)
            .searchSourceBuilder(searchSourceBuilder)
            .build();

        doAnswer(invocation -> {
            ActionListener<SearchResponse> listener = invocation.getArgument(1);
            listener.onFailure(new UnsupportedOperationException("test"));
            return null;
        }).when(mockedClient).search(any(SearchRequest.class), any());

        CompletableFuture<SearchDataObjectResponse> future = sdkClient.searchDataObjectAsync(searchRequest).toCompletableFuture();

        CompletionException ce = assertThrows(CompletionException.class, () -> future.join());
        Throwable cause = ce.getCause();
        assertEquals(OpenSearchStatusException.class, cause.getClass());
        assertEquals(RestStatus.INTERNAL_SERVER_ERROR, ((OpenSearchStatusException) cause).status());
        assertEquals("Failed to search indices [test_index]", cause.getMessage());
    }

    @Test
    public void testSearchDataObject_NullTenantNoMultitenancy() throws IOException {
        // Tests no status exception if multitenancy not enabled
        LocalClusterIndicesClient innerClient = new LocalClusterIndicesClient(
            mockedClient,
            xContentRegistry,
            Map.of(TENANT_ID_FIELD_KEY, TENANT_ID_FIELD)
        );
        SdkClient sdkClientNoTenant = new SdkClient(innerClient, false, null);

        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        SearchDataObjectRequest searchRequest = SearchDataObjectRequest.builder()
            .indices(TEST_INDEX)
            // null tenant Id
            .searchSourceBuilder(searchSourceBuilder)
            .build();

        doAnswer(invocation -> {
            ActionListener<SearchResponse> listener = invocation.getArgument(1);
            listener.onFailure(new UnsupportedOperationException("test"));
            return null;
        }).when(mockedClient).search(any(SearchRequest.class), any());

        CompletableFuture<SearchDataObjectResponse> future = sdkClientNoTenant.searchDataObjectAsync(searchRequest).toCompletableFuture();

        CompletionException ce = assertThrows(CompletionException.class, () -> future.join());
        Throwable cause = ce.getCause();
        assertEquals(OpenSearchStatusException.class, cause.getClass());
        assertEquals("Failed to search indices [test_index]", cause.getMessage());
    }

    @Test
    public void testGetDataAsync_WithGlobalTenantId_resourceFound() {
        String json = "{\"" + TENANT_ID_FIELD + "\":\"" + GLOBAL_TENANT_ID + "\"}";
        GetResponse getResponse = new GetResponse(new GetResult(TEST_INDEX, TEST_ID, -2, 0, 1, true, new BytesArray(json), null, null));
        innerClient.setGlobalTenantId(GLOBAL_TENANT_ID);
        doAnswer(invocation -> {
            ActionListener<GetResponse> listener = invocation.getArgument(1);
            listener.onResponse(getResponse);
            return null;
        }).when(mockedClient).get(any(GetRequest.class), any());
        SdkClient sdkClient = new SdkClient(innerClient, true, GLOBAL_TENANT_ID);
        GetDataObjectResponse result = sdkClient.getDataObjectAsync(
            GetDataObjectRequest.builder().index(TEST_INDEX).id(TEST_ID).tenantId(TEST_TENANT_ID).build()
        ).toCompletableFuture().join();
        assertTrue(Objects.requireNonNull(result.getResponse()).isExists());
        assertEquals(TEST_TENANT_ID, Objects.requireNonNull(result.getResponse()).getSourceAsMap().get(TENANT_ID_FIELD_KEY));
    }

    @Test
    public void testGetDataAsync_WithGlobalTenantId_notGlobalResource() {
        String json = "{\"" + TENANT_ID_FIELD + "\":\"" + TEST_TENANT_ID + "\"}";
        GetResponse getResponse = new GetResponse(new GetResult(TEST_INDEX, TEST_ID, -2, 0, 1, true, new BytesArray(json), null, null));
        innerClient.setGlobalTenantId(GLOBAL_TENANT_ID);
        doAnswer(invocation -> {
            ActionListener<GetResponse> listener = invocation.getArgument(1);
            listener.onResponse(getResponse);
            return null;
        }).when(mockedClient).get(any(GetRequest.class), any());
        SdkClient sdkClient = new SdkClient(innerClient, true, GLOBAL_TENANT_ID);
        GetDataObjectResponse result = sdkClient.getDataObjectAsync(
            GetDataObjectRequest.builder().index(TEST_INDEX).id(TEST_ID).tenantId(TEST_TENANT_ID).build()
        ).toCompletableFuture().join();
        assertTrue(Objects.requireNonNull(result.getResponse()).isExists());
        assertEquals(TEST_TENANT_ID, Objects.requireNonNull(result.getResponse()).getSourceAsMap().get(TENANT_ID_FIELD_KEY));
    }

    @Test
    public void testGetDataAsync_WithGlobalTenantId_foundFromCache() {
        String json = "{\"" + TENANT_ID_FIELD + "\":\"" + GLOBAL_TENANT_ID + "\"}";
        GetResponse getResponse = new GetResponse(new GetResult(TEST_INDEX, TEST_ID, -2, 0, 1, true, new BytesArray(json), null, null));
        innerClient.setGlobalTenantId(GLOBAL_TENANT_ID);
        doAnswer(invocation -> {
            ActionListener<GetResponse> listener = invocation.getArgument(1);
            listener.onResponse(getResponse);
            return null;
        }).when(mockedClient).get(any(GetRequest.class), any());
        SdkClient sdkClient = new SdkClient(innerClient, true, GLOBAL_TENANT_ID);
        GetDataObjectResponse result = innerClient.getDataObjectAsync(
            GetDataObjectRequest.builder().index(TEST_INDEX).id(TEST_ID).tenantId(TEST_TENANT_ID).build(),
            null,
            null
        ).toCompletableFuture().join();
        assertTrue(Objects.requireNonNull(result.getResponse()).isExists());

        GetDataObjectResponse resultFromCache = sdkClient.getDataObjectAsync(
            GetDataObjectRequest.builder().index(TEST_INDEX).id(TEST_ID).tenantId(TEST_TENANT_ID).build()
        ).toCompletableFuture().join();
        assertTrue(Objects.requireNonNull(resultFromCache.getResponse()).isExists());
        assertEquals(TEST_TENANT_ID, Objects.requireNonNull(result.getResponse()).getSourceAsMap().get(TENANT_ID_FIELD_KEY));
    }

    @Test
    public void testGetDataAsync_WithUserTenantId_resourceFound() {
        String json = "{\"" + TENANT_ID_FIELD + "\":\"" + TEST_TENANT_ID + "\"}";
        GetResponse getResponse = new GetResponse(new GetResult(TEST_INDEX, TEST_ID, -2, 0, 1, true, new BytesArray(json), null, null));
        doAnswer(invocation -> {
            ActionListener<GetResponse> listener = invocation.getArgument(1);
            listener.onResponse(getResponse);
            return null;
        }).when(mockedClient).get(any(GetRequest.class), any());
        SdkClient sdkClient = new SdkClient(innerClient, true, GLOBAL_TENANT_ID);
        GetDataObjectResponse result = sdkClient.getDataObjectAsync(
            GetDataObjectRequest.builder().index(TEST_INDEX).id(TEST_ID).tenantId(TEST_TENANT_ID).build()
        ).toCompletableFuture().join();
        assertTrue(Objects.requireNonNull(result.getResponse()).isExists());
        assertEquals(TEST_TENANT_ID, Objects.requireNonNull(result.getResponse()).getSourceAsMap().get(TENANT_ID_FIELD_KEY));
    }

    @Test
    public void testIsGlobalResource_WithGlobalTenantId() {
        String json = "{\"" + TENANT_ID_FIELD + "\":\"" + GLOBAL_TENANT_ID + "\"}";
        GetResponse getResponse = new GetResponse(new GetResult(TEST_INDEX, TEST_ID, -2, 0, 1, true, new BytesArray(json), null, null));
        innerClient.setGlobalTenantId(GLOBAL_TENANT_ID);
        doAnswer(invocation -> {
            ActionListener<GetResponse> listener = invocation.getArgument(1);
            listener.onResponse(getResponse);
            return null;
        }).when(mockedClient).get(any(GetRequest.class), any());
        SdkClient sdkClient = new SdkClient(innerClient, true, GLOBAL_TENANT_ID);
        boolean result = sdkClient.isGlobalResource(TEST_INDEX, TEST_ID).toCompletableFuture().join();
        assertTrue(result);

        // Verify the request
        ArgumentCaptor<GetRequest> requestCaptor = ArgumentCaptor.forClass(GetRequest.class);
        verify(mockedClient, times(1)).get(requestCaptor.capture(), any());
        assertEquals(TEST_INDEX, requestCaptor.getValue().index());
        assertEquals(TEST_ID, requestCaptor.getValue().id());
    }

    @Test
    public void testIsGlobalResource_resourceWithNonGlobalTenantId() {
        String json = "{\"" + TENANT_ID_FIELD + "\":\"" + TEST_TENANT_ID + "\"}";
        GetResponse getResponse = new GetResponse(new GetResult(TEST_INDEX, TEST_ID, -2, 0, 1, true, new BytesArray(json), null, null));
        innerClient.setGlobalTenantId(GLOBAL_TENANT_ID);
        doAnswer(invocation -> {
            ActionListener<GetResponse> listener = invocation.getArgument(1);
            listener.onResponse(getResponse);
            return null;
        }).when(mockedClient).get(any(GetRequest.class), any());
        SdkClient sdkClient = new SdkClient(innerClient, true, GLOBAL_TENANT_ID);
        boolean result = sdkClient.isGlobalResource(TEST_INDEX, TEST_ID).toCompletableFuture().join();
        assertFalse(result);
    }

    @Test
    public void testIsGlobalResource_DocumentDoesNotExist() {
        GetResponse getResponse = new GetResponse(new GetResult(TEST_INDEX, "nonExistId", -2, 0, 1, false, null, null, null));
        innerClient.setGlobalTenantId(GLOBAL_TENANT_ID);
        doAnswer(invocation -> {
            ActionListener<GetResponse> listener = invocation.getArgument(1);
            listener.onResponse(getResponse);
            return null;
        }).when(mockedClient).get(any(GetRequest.class), any());
        SdkClient sdkClient = new SdkClient(innerClient, true, GLOBAL_TENANT_ID);
        boolean result = sdkClient.isGlobalResource(TEST_INDEX, TEST_ID).toCompletableFuture().join();
        assertFalse(result);
    }

    @Test
    public void testIsGlobalResource_Exception() {
        innerClient.setGlobalTenantId(GLOBAL_TENANT_ID);
        // Mock an exception
        doAnswer(invocation -> {
            ActionListener<GetResponse> listener = invocation.getArgument(1);
            listener.onFailure(new RuntimeException("Test exception"));
            return null;
        }).when(mockedClient).get(any(GetRequest.class), any());
        SdkClient sdkClient = new SdkClient(innerClient, true, GLOBAL_TENANT_ID);
        // Execute the method and verify result - should return false on exception
        CompletableFuture<Boolean> future = sdkClient.isGlobalResource(TEST_INDEX, TEST_ID).toCompletableFuture();
        CompletionException ce = assertThrows(CompletionException.class, () -> future.join());
        Throwable cause = ce.getCause();
        assertEquals(OpenSearchStatusException.class, cause.getClass());
        assertEquals("Failed to get data object from index test_index", cause.getMessage());
    }

    @Test
    public void testIsGlobalResource_NoTenantIdField() {
        String json = "{\"other_field\":\"some_value\"}";
        GetResponse getResponse = new GetResponse(new GetResult(TEST_INDEX, TEST_ID, -2, 0, 1, true, new BytesArray(json), null, null));
        innerClient.setGlobalTenantId(GLOBAL_TENANT_ID);
        doAnswer(invocation -> {
            ActionListener<GetResponse> listener = invocation.getArgument(1);
            listener.onResponse(getResponse);
            return null;
        }).when(mockedClient).get(any(GetRequest.class), any());
        SdkClient sdkClient = new SdkClient(innerClient, true, GLOBAL_TENANT_ID);
        boolean result = sdkClient.isGlobalResource(TEST_INDEX, TEST_ID).toCompletableFuture().join();
        assertFalse(result);
    }

    @Test
    public void testHandleOSDocumentBasedResponse_globalResourceEnabled_globalResourceFound() {
        String json = "{\"" + TENANT_ID_FIELD + "\":\"" + GLOBAL_TENANT_ID + "\"}";
        GetResponse getResponse = new GetResponse(new GetResult(TEST_INDEX, TEST_ID, -2, 0, 1, true, new BytesArray(json), null, null));
        innerClient.setGlobalTenantId(GLOBAL_TENANT_ID);
        GetDataObjectRequest request = mock(GetDataObjectRequest.class);
        when(request.tenantId()).thenReturn(TEST_TENANT_ID);
        GetDataObjectResponse response = mock(GetDataObjectResponse.class);
        when(response.getResponse()).thenReturn(getResponse);
        CompletionStage<GetDataObjectResponse> getDataObjectResponseCompletionStage = CompletableFuture.completedStage(response);
        GetDataObjectResponse handledResponse = innerClient.handleOSDocumentBasedResponse(request, getDataObjectResponseCompletionStage)
            .toCompletableFuture()
            .join();
        assertEquals(TEST_TENANT_ID, Objects.requireNonNull(handledResponse.getResponse()).getSourceAsMap().get(TENANT_ID_FIELD_KEY));
    }

    @Test
    public void testHandleOSDocumentBasedResponse_globalResourceEnabled_resourceNotFound() {
        GetResponse getResponse = new GetResponse(new GetResult(TEST_INDEX, TEST_ID, -2, 0, 1, false, null, null, null));
        innerClient.setGlobalTenantId(GLOBAL_TENANT_ID);
        GetDataObjectRequest request = mock(GetDataObjectRequest.class);
        GetDataObjectResponse response = mock(GetDataObjectResponse.class);
        when(response.getResponse()).thenReturn(getResponse);
        CompletionStage<GetDataObjectResponse> getDataObjectResponseCompletionStage = CompletableFuture.completedStage(response);
        GetDataObjectResponse handledResponse = innerClient.handleOSDocumentBasedResponse(request, getDataObjectResponseCompletionStage)
            .toCompletableFuture()
            .join();
        assertFalse(Objects.requireNonNull(handledResponse.getResponse()).isExists());
    }

    @Test
    public void testHandleOSDocumentBasedResponse_globalResourceEnabled_userResourceFound() {
        String json = "{\"" + TENANT_ID_FIELD + "\":\"" + TEST_TENANT_ID + "\"}";
        GetResponse getResponse = new GetResponse(new GetResult(TEST_INDEX, TEST_ID, -2, 0, 1, true, new BytesArray(json), null, null));
        innerClient.setGlobalTenantId(GLOBAL_TENANT_ID);
        GetDataObjectRequest request = mock(GetDataObjectRequest.class);
        GetDataObjectResponse response = mock(GetDataObjectResponse.class);
        when(response.getResponse()).thenReturn(getResponse);
        CompletionStage<GetDataObjectResponse> getDataObjectResponseCompletionStage = CompletableFuture.completedStage(response);
        GetDataObjectResponse handledResponse = innerClient.handleOSDocumentBasedResponse(request, getDataObjectResponseCompletionStage)
            .toCompletableFuture()
            .join();
        assertEquals(TEST_TENANT_ID, Objects.requireNonNull(handledResponse.getResponse()).getSourceAsMap().get(TENANT_ID_FIELD_KEY));
    }
}
