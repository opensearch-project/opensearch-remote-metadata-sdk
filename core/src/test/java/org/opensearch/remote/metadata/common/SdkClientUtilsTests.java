/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.remote.metadata.common;

import org.opensearch.OpenSearchException;
import org.opensearch.OpenSearchStatusException;
import org.opensearch.action.DocWriteResponse.Result;
import org.opensearch.action.bulk.BulkItemResponse;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.action.delete.DeleteResponse;
import org.opensearch.action.get.GetResponse;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.action.search.SearchPhaseName;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.search.ShardSearchFailure;
import org.opensearch.action.support.PlainActionFuture;
import org.opensearch.action.support.replication.ReplicationResponse.ShardInfo;
import org.opensearch.action.update.UpdateResponse;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.index.get.GetResult;
import org.opensearch.remote.metadata.client.BulkDataObjectResponse;
import org.opensearch.remote.metadata.client.DeleteDataObjectResponse;
import org.opensearch.remote.metadata.client.GetDataObjectResponse;
import org.opensearch.remote.metadata.client.PutDataObjectResponse;
import org.opensearch.remote.metadata.client.SearchDataObjectResponse;
import org.opensearch.remote.metadata.client.UpdateDataObjectResponse;
import org.opensearch.search.internal.InternalSearchResponse;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.EnumSet;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import org.mockito.ArgumentCaptor;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class SdkClientUtilsTests {
    private static final String TEST_ID = "123";
    private static final String TEST_INDEX = "test_index";

    private OpenSearchStatusException testException;
    private InterruptedException interruptedException;
    private IOException ioException;
    private TestDataObject testDataObject;

    @BeforeEach
    public void setUp() {
        testException = new OpenSearchStatusException("Test", RestStatus.BAD_REQUEST);
        interruptedException = new InterruptedException();
        ioException = new IOException();
        testDataObject = new TestDataObject("foo");
    }

    @Test
    void testLowerCaseEnumValues() {
        // Test normal case
        String input = "{\"status\":\"CREATED\"}";
        String result = SdkClientUtils.lowerCaseEnumValues("status", input);
        assertEquals("{\"status\":\"created\"}", result);

        // Test with multiple occurrences
        input = "{\"status\":\"CREATED\",\"other\":\"UPDATED\"}";
        result = SdkClientUtils.lowerCaseEnumValues("status", input);
        assertEquals("{\"status\":\"created\",\"other\":\"UPDATED\"}", result);

        // Test with no matches
        input = "{\"status\":\"created\"}";
        result = SdkClientUtils.lowerCaseEnumValues("status", input);
        assertEquals(input, result);

        // Test with empty string
        assertEquals("", SdkClientUtils.lowerCaseEnumValues("status", ""));

        // Test null cases
        assertNull(SdkClientUtils.lowerCaseEnumValues("status", null));
        assertEquals("some json", SdkClientUtils.lowerCaseEnumValues(null, "some json"));

        // Test with mixed case (should only match all caps)
        input = "{\"status\":\"Created\"}";
        result = SdkClientUtils.lowerCaseEnumValues("status", input);
        assertEquals(input, result);
    }

    @Test
    void testWrapPutCompletion_Failure() {
        @SuppressWarnings("unchecked")
        ActionListener<IndexResponse> listener = mock(ActionListener.class);
        CompletableFuture<PutDataObjectResponse> future = new CompletableFuture<>();
        future.completeExceptionally(new CompletionException(testException));

        future.whenComplete(SdkClientUtils.wrapPutCompletion(listener));

        verify(listener).onFailure(testException);
    }

    @Test
    void testWrapPutCompletion_NullParser() {
        @SuppressWarnings("unchecked")
        ActionListener<IndexResponse> listener = mock(ActionListener.class);
        PutDataObjectResponse response = mock(PutDataObjectResponse.class);
        when(response.parser()).thenReturn(null);
        CompletableFuture<PutDataObjectResponse> future = CompletableFuture.completedFuture(response);

        future.whenComplete(SdkClientUtils.wrapPutCompletion(listener));

        verify(listener).onFailure(any(OpenSearchException.class));
    }

    @Test
    void testWrapPutCompletion_ParseFailure() throws IOException {
        @SuppressWarnings("unchecked")
        ActionListener<IndexResponse> listener = mock(ActionListener.class);
        PutDataObjectResponse response = mock(PutDataObjectResponse.class);
        XContentParser parser = mock(XContentParser.class);
        when(parser.nextToken()).thenThrow(new IOException("Test IO Exception"));
        when(response.parser()).thenReturn(parser);
        CompletableFuture<PutDataObjectResponse> future = CompletableFuture.completedFuture(response);

        future.whenComplete(SdkClientUtils.wrapPutCompletion(listener));

        ArgumentCaptor<Exception> exceptionCaptor = ArgumentCaptor.forClass(Exception.class);
        verify(listener).onFailure(exceptionCaptor.capture());
        Exception capturedException = exceptionCaptor.getValue();
        assertTrue(capturedException instanceof OpenSearchStatusException);
    }

    @Test
    void testWrapGetCompletion_Success() throws IOException {
        @SuppressWarnings("unchecked")
        ActionListener<GetResponse> listener = mock(ActionListener.class);
        GetDataObjectResponse response = mock(GetDataObjectResponse.class);
        XContentBuilder builder = JsonXContent.contentBuilder();
        String json = testDataObject.toJson();
        new GetResponse(new GetResult(TEST_INDEX, TEST_ID, -2, 0, 1, true, new BytesArray(json), null, null)).toXContent(
            builder,
            ToXContent.EMPTY_PARAMS
        );
        XContentParser parser = createParser(builder);
        when(response.parser()).thenReturn(parser);
        when(response.getResponse()).thenCallRealMethod();
        CompletableFuture<GetDataObjectResponse> future = CompletableFuture.completedFuture(response);

        future.whenComplete(SdkClientUtils.wrapGetCompletion(listener));

        verify(listener).onResponse(any(GetResponse.class));
    }

    @Test
    void testWrapGetCompletion_Failure() {
        @SuppressWarnings("unchecked")
        ActionListener<GetResponse> listener = mock(ActionListener.class);
        CompletableFuture<GetDataObjectResponse> future = new CompletableFuture<>();
        future.completeExceptionally(new CompletionException(testException));

        future.whenComplete(SdkClientUtils.wrapGetCompletion(listener));

        verify(listener).onFailure(testException);
    }

    @Test
    void testWrapGetCompletion_NullParser() {
        @SuppressWarnings("unchecked")
        ActionListener<GetResponse> listener = mock(ActionListener.class);
        GetDataObjectResponse response = mock(GetDataObjectResponse.class);
        when(response.parser()).thenReturn(null);
        CompletableFuture<GetDataObjectResponse> future = CompletableFuture.completedFuture(response);

        future.whenComplete(SdkClientUtils.wrapGetCompletion(listener));

        verify(listener).onFailure(any(OpenSearchException.class));
    }

    @Test
    void testWrapGetCompletion_ParseFailure() throws IOException {
        @SuppressWarnings("unchecked")
        ActionListener<GetResponse> listener = mock(ActionListener.class);
        GetDataObjectResponse response = mock(GetDataObjectResponse.class);
        XContentParser parser = mock(XContentParser.class);
        when(parser.nextToken()).thenThrow(new IOException("Test IO Exception"));
        when(response.parser()).thenReturn(parser);
        CompletableFuture<GetDataObjectResponse> future = CompletableFuture.completedFuture(response);

        future.whenComplete(SdkClientUtils.wrapGetCompletion(listener));

        ArgumentCaptor<Exception> exceptionCaptor = ArgumentCaptor.forClass(Exception.class);
        verify(listener).onFailure(exceptionCaptor.capture());
        Exception capturedException = exceptionCaptor.getValue();
        assertTrue(capturedException instanceof OpenSearchStatusException);
    }

    @Test
    void testWrapUpdateCompletion_Success() throws IOException {
        @SuppressWarnings("unchecked")
        ActionListener<UpdateResponse> listener = mock(ActionListener.class);
        UpdateDataObjectResponse response = mock(UpdateDataObjectResponse.class);
        XContentBuilder builder = JsonXContent.contentBuilder();
        new UpdateResponse(new ShardInfo(1, 1), new ShardId(TEST_INDEX, "_na_", 0), TEST_ID, 1, 0, 2, Result.UPDATED).toXContent(
            builder,
            ToXContent.EMPTY_PARAMS
        );
        XContentParser parser = createParser(builder);
        when(response.parser()).thenReturn(parser);
        when(response.updateResponse()).thenCallRealMethod();
        CompletableFuture<UpdateDataObjectResponse> future = CompletableFuture.completedFuture(response);

        future.whenComplete(SdkClientUtils.wrapUpdateCompletion(listener));

        verify(listener).onResponse(any(UpdateResponse.class));
    }

    @Test
    void testWrapUpdateCompletion_Failure() {
        @SuppressWarnings("unchecked")
        ActionListener<UpdateResponse> listener = mock(ActionListener.class);
        CompletableFuture<UpdateDataObjectResponse> future = new CompletableFuture<>();
        future.completeExceptionally(new CompletionException(testException));

        future.whenComplete(SdkClientUtils.wrapUpdateCompletion(listener));

        verify(listener).onFailure(testException);
    }

    @Test
    void testWrapUpdateCompletion_NullParser() {
        @SuppressWarnings("unchecked")
        ActionListener<UpdateResponse> listener = mock(ActionListener.class);
        UpdateDataObjectResponse response = mock(UpdateDataObjectResponse.class);
        when(response.parser()).thenReturn(null);
        CompletableFuture<UpdateDataObjectResponse> future = CompletableFuture.completedFuture(response);

        future.whenComplete(SdkClientUtils.wrapUpdateCompletion(listener));

        verify(listener).onFailure(any(OpenSearchException.class));
    }

    @Test
    void testWrapUpdateCompletion_ParseFailure() throws IOException {
        @SuppressWarnings("unchecked")
        ActionListener<UpdateResponse> listener = mock(ActionListener.class);
        UpdateDataObjectResponse response = mock(UpdateDataObjectResponse.class);
        XContentParser parser = mock(XContentParser.class);
        when(parser.nextToken()).thenThrow(new IOException("Test IO Exception"));
        when(response.parser()).thenReturn(parser);
        CompletableFuture<UpdateDataObjectResponse> future = CompletableFuture.completedFuture(response);

        future.whenComplete(SdkClientUtils.wrapUpdateCompletion(listener));

        ArgumentCaptor<Exception> exceptionCaptor = ArgumentCaptor.forClass(Exception.class);
        verify(listener).onFailure(exceptionCaptor.capture());
        Exception capturedException = exceptionCaptor.getValue();
        assertTrue(capturedException instanceof OpenSearchStatusException);
    }

    @Test
    void testWrapDeleteCompletion_Success() throws IOException {
        @SuppressWarnings("unchecked")
        ActionListener<DeleteResponse> listener = mock(ActionListener.class);
        DeleteDataObjectResponse response = mock(DeleteDataObjectResponse.class);
        XContentBuilder builder = JsonXContent.contentBuilder();
        new DeleteResponse(new ShardId(TEST_INDEX, "_na_", 0), TEST_ID, 1, 0, 2, true).toXContent(builder, ToXContent.EMPTY_PARAMS);
        XContentParser parser = createParser(builder);
        when(response.parser()).thenReturn(parser);
        when(response.deleteResponse()).thenCallRealMethod();
        CompletableFuture<DeleteDataObjectResponse> future = CompletableFuture.completedFuture(response);

        future.whenComplete(SdkClientUtils.wrapDeleteCompletion(listener));

        verify(listener).onResponse(any(DeleteResponse.class));
    }

    @Test
    void testWrapDeleteCompletion_Failure() {
        @SuppressWarnings("unchecked")
        ActionListener<DeleteResponse> listener = mock(ActionListener.class);
        CompletableFuture<DeleteDataObjectResponse> future = new CompletableFuture<>();
        future.completeExceptionally(new CompletionException(testException));

        future.whenComplete(SdkClientUtils.wrapDeleteCompletion(listener));

        verify(listener).onFailure(testException);
    }

    @Test
    void testWrapDeleteCompletion_NullParser() {
        @SuppressWarnings("unchecked")
        ActionListener<DeleteResponse> listener = mock(ActionListener.class);
        DeleteDataObjectResponse response = mock(DeleteDataObjectResponse.class);
        when(response.parser()).thenReturn(null);
        CompletableFuture<DeleteDataObjectResponse> future = CompletableFuture.completedFuture(response);

        future.whenComplete(SdkClientUtils.wrapDeleteCompletion(listener));

        verify(listener).onFailure(any(OpenSearchException.class));
    }

    @Test
    void testWrapDeleteCompletion_ParseFailure() throws IOException {
        @SuppressWarnings("unchecked")
        ActionListener<DeleteResponse> listener = mock(ActionListener.class);
        DeleteDataObjectResponse response = mock(DeleteDataObjectResponse.class);
        XContentParser parser = mock(XContentParser.class);
        when(parser.nextToken()).thenThrow(new IOException("Test IO Exception"));
        when(response.parser()).thenReturn(parser);
        CompletableFuture<DeleteDataObjectResponse> future = CompletableFuture.completedFuture(response);

        future.whenComplete(SdkClientUtils.wrapDeleteCompletion(listener));

        ArgumentCaptor<Exception> exceptionCaptor = ArgumentCaptor.forClass(Exception.class);
        verify(listener).onFailure(exceptionCaptor.capture());
        Exception capturedException = exceptionCaptor.getValue();
        assertTrue(capturedException instanceof OpenSearchStatusException);
    }

    @Test
    void testWrapBulkCompletion_Success() throws IOException, InterruptedException, ExecutionException {
        @SuppressWarnings("unchecked")
        ActionListener<BulkResponse> listener = mock(ActionListener.class);
        BulkDataObjectResponse response = mock(BulkDataObjectResponse.class);
        when(response.bulkResponse()).thenReturn(new BulkResponse(new BulkItemResponse[0], 100L));
        CompletableFuture<BulkDataObjectResponse> future = CompletableFuture.completedFuture(response);

        future.whenComplete(SdkClientUtils.wrapBulkCompletion(listener));

        verify(listener).onResponse(any(BulkResponse.class));
    }

    @Test
    void testWrapBulkCompletion_Failure() {
        @SuppressWarnings("unchecked")
        ActionListener<BulkResponse> listener = mock(ActionListener.class);
        CompletableFuture<BulkDataObjectResponse> future = new CompletableFuture<>();
        future.completeExceptionally(new CompletionException(testException));

        future.whenComplete(SdkClientUtils.wrapBulkCompletion(listener));

        verify(listener).onFailure(testException);
    }

    @Test
    void testWrapBulkCompletion_NullParser() {
        @SuppressWarnings("unchecked")
        ActionListener<BulkResponse> listener = mock(ActionListener.class);
        BulkDataObjectResponse response = mock(BulkDataObjectResponse.class);
        when(response.parser()).thenReturn(null);
        CompletableFuture<BulkDataObjectResponse> future = CompletableFuture.completedFuture(response);

        future.whenComplete(SdkClientUtils.wrapBulkCompletion(listener));

        verify(listener).onFailure(any(OpenSearchException.class));
    }

    @Test
    void testWrapBulkCompletion_ParseFailure() throws IOException {
        @SuppressWarnings("unchecked")
        ActionListener<BulkResponse> listener = mock(ActionListener.class);
        BulkDataObjectResponse response = mock(BulkDataObjectResponse.class);
        XContentParser parser = mock(XContentParser.class);
        when(parser.nextToken()).thenThrow(new IOException("Test IO Exception"));
        when(response.parser()).thenReturn(parser);
        CompletableFuture<BulkDataObjectResponse> future = CompletableFuture.completedFuture(response);

        future.whenComplete(SdkClientUtils.wrapBulkCompletion(listener));

        ArgumentCaptor<Exception> exceptionCaptor = ArgumentCaptor.forClass(Exception.class);
        verify(listener).onFailure(exceptionCaptor.capture());
        Exception capturedException = exceptionCaptor.getValue();
        assertTrue(capturedException instanceof OpenSearchStatusException);
    }

    @Test
    void testWrapSearchCompletion_Success() throws IOException {
        @SuppressWarnings("unchecked")
        ActionListener<SearchResponse> listener = mock(ActionListener.class);
        XContentBuilder builder = JsonXContent.contentBuilder();
        new SearchResponse(
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
        ).toXContent(builder, ToXContent.EMPTY_PARAMS);
        XContentParser parser = createParser(builder);
        SearchDataObjectResponse response = SearchDataObjectResponse.builder().parser(parser).build();
        CompletableFuture<SearchDataObjectResponse> future = CompletableFuture.completedFuture(response);

        future.whenComplete(SdkClientUtils.wrapSearchCompletion(listener));

        verify(listener).onResponse(any(SearchResponse.class));
    }

    @Test
    void testWrapSearchCompletion_Failure() {
        @SuppressWarnings("unchecked")
        ActionListener<SearchResponse> listener = mock(ActionListener.class);
        CompletableFuture<SearchDataObjectResponse> future = new CompletableFuture<>();
        future.completeExceptionally(new CompletionException(testException));

        future.whenComplete(SdkClientUtils.wrapSearchCompletion(listener));

        verify(listener).onFailure(testException);
    }

    @Test
    void testWrapSearchCompletion_ParseFailure() throws IOException {
        @SuppressWarnings("unchecked")
        ActionListener<SearchResponse> listener = mock(ActionListener.class);
        XContentParser parser = mock(XContentParser.class);
        when(parser.nextToken()).thenThrow(new IOException("Test IO Exception"));
        SearchDataObjectResponse response = SearchDataObjectResponse.builder().parser(parser).build();
        CompletableFuture<SearchDataObjectResponse> future = CompletableFuture.completedFuture(response);

        future.whenComplete(SdkClientUtils.wrapSearchCompletion(listener));

        ArgumentCaptor<Exception> exceptionCaptor = ArgumentCaptor.forClass(Exception.class);
        verify(listener).onFailure(exceptionCaptor.capture());
        Exception capturedException = exceptionCaptor.getValue();
        assertTrue(capturedException instanceof OpenSearchStatusException);
    }

    @Test
    public void testUnwrapAndConvertToException_CompletionException() {
        CompletionException ce = new CompletionException(testException);
        Exception e = SdkClientUtils.unwrapAndConvertToException(ce);
        assertSame(testException, e);

        ce = new CompletionException(interruptedException);
        e = SdkClientUtils.unwrapAndConvertToException(ce); // sets interrupted
        assertTrue(Thread.interrupted()); // tests and resets interrupted
        assertSame(interruptedException, e);

        ce = new CompletionException(ioException);
        e = SdkClientUtils.unwrapAndConvertToException(ce);
        assertFalse(Thread.currentThread().isInterrupted());
        assertSame(ioException, e);

        PlainActionFuture<Object> future = PlainActionFuture.newFuture();
        future.onFailure(ioException);
        e = assertThrows(RuntimeException.class, () -> future.actionGet());
        e = SdkClientUtils.unwrapAndConvertToException(e);
        assertSame(ioException, e);
    }

    @Test
    public void testUnwrapAndConvertToException_Unwrapped() {
        CancellationException ce = new CancellationException();
        Exception e = SdkClientUtils.unwrapAndConvertToException(ce);
        assertSame(ce, e);

        e = SdkClientUtils.unwrapAndConvertToException(ioException);
        assertSame(ioException, e);
    }

    @Test
    public void testUnwrapAndConvertToException_VarargsUnwrap() {
        // Create nested exceptions
        OpenSearchException openSearchException = new OpenSearchException("Custom exception");
        CompletionException completionException = new CompletionException(openSearchException);
        OpenSearchStatusException statusException = new OpenSearchStatusException(
            "Status exception",
            RestStatus.INTERNAL_SERVER_ERROR,
            completionException
        );

        // Test unwrapping with multiple exception types
        Exception result = SdkClientUtils.unwrapAndConvertToException(
            statusException,
            OpenSearchStatusException.class,
            CompletionException.class,
            OpenSearchException.class
        );
        assertSame(openSearchException, result, "Should unwrap to the OpenSearchException");

        // Test with a different order of exception types (order shouldn't matter now)
        result = SdkClientUtils.unwrapAndConvertToException(
            statusException,
            CompletionException.class,
            OpenSearchException.class,
            OpenSearchStatusException.class
        );
        assertSame(openSearchException, result, "Should still unwrap to the OpenSearchException regardless of order");

        // Test with only one exception type
        result = SdkClientUtils.unwrapAndConvertToException(statusException, OpenSearchStatusException.class);
        assertSame(completionException, result, "Should unwrap to the CompletionException (cause of OpenSearchStatusException)");

        // Test with no matching exception type
        IOException ioException = new IOException("IO Exception");
        result = SdkClientUtils.unwrapAndConvertToException(ioException, OpenSearchException.class, CompletionException.class);
        assertSame(ioException, result, "Should return the original exception when no matching type is found");

        // Test with default behavior (only CompletionException)
        result = SdkClientUtils.unwrapAndConvertToException(completionException);
        assertSame(openSearchException, result, "Should unwrap CompletionException by default");

        // Test with InterruptedException
        InterruptedException interruptedException = new InterruptedException("Interrupted");
        result = SdkClientUtils.unwrapAndConvertToException(interruptedException);
        assertSame(interruptedException, result, "Should return InterruptedException and set interrupt flag");
        assertTrue(Thread.interrupted(), "Interrupt flag should be set");

        // Test with a non-Exception Throwable
        Error error = new Error("Some error");
        result = SdkClientUtils.unwrapAndConvertToException(error);
        assertTrue(result instanceof OpenSearchException, "Should wrap non-Exception Throwable in OpenSearchException");
        assertSame(error, result.getCause(), "Wrapped OpenSearchException should have original Error as cause");
    }

    @Test
    public void testGetRethrownExecutionException_Unwrapped() {
        PlainActionFuture<Object> future = PlainActionFuture.newFuture();
        future.onFailure(testException);
        RuntimeException e = assertThrows(RuntimeException.class, () -> future.actionGet());
        Throwable notWrapped = SdkClientUtils.getRethrownExecutionExceptionRootCause(e);
        assertSame(testException, notWrapped);
    }

    @Test
    public void testGetRethrownExecutionException_Wrapped() {
        PlainActionFuture<Object> future = PlainActionFuture.newFuture();
        future.onFailure(ioException);
        RuntimeException e = assertThrows(RuntimeException.class, () -> future.actionGet());
        Throwable wrapped = SdkClientUtils.getRethrownExecutionExceptionRootCause(e);
        assertSame(ioException, wrapped);
    }

    @Test
    void testCreateParser_FromToXContent() throws IOException {
        ToXContentObject simpleObject = new ToXContentObject() {
            @Override
            public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
                return builder.startObject().field("key", "value").endObject();
            }
        };

        XContentParser parser = SdkClientUtils.createParser(simpleObject);
        assertEquals(XContentParser.Token.START_OBJECT, parser.nextToken());
        assertEquals(XContentParser.Token.FIELD_NAME, parser.nextToken());
        assertEquals("key", parser.currentName());
        assertEquals(XContentParser.Token.VALUE_STRING, parser.nextToken());
        assertEquals("value", parser.text());
        assertEquals(XContentParser.Token.END_OBJECT, parser.nextToken());
        assertNull(parser.nextToken());
    }

    @Test
    void testCreateParser_FromString() throws IOException {
        String json = "{\"key\":\"value\"}";
        XContentParser parser = SdkClientUtils.createParser(json);
        assertEquals(XContentParser.Token.START_OBJECT, parser.nextToken());
        assertEquals(XContentParser.Token.FIELD_NAME, parser.nextToken());
        assertEquals("key", parser.currentName());
        assertEquals(XContentParser.Token.VALUE_STRING, parser.nextToken());
        assertEquals("value", parser.text());
        assertEquals(XContentParser.Token.END_OBJECT, parser.nextToken());
        assertNull(parser.nextToken());
    }

    @Test
    void testCreateParser_NullInput() {
        assertThrows(NullPointerException.class, () -> SdkClientUtils.createParser((ToXContent) null));
        assertThrows(NullPointerException.class, () -> SdkClientUtils.createParser((String) null));
    }

    @Test
    void testCreateParser_InvalidJson() throws IOException {
        String invalidJson = "invalid json";
        XContentParser parser = SdkClientUtils.createParser(invalidJson);
        assertThrows(IOException.class, () -> parser.nextToken());
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
            + "    \"sterms#unique_connector_names\": {\n"
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
        XContentParser parser = SdkClientUtils.createParser(searchResponseJson);
        org.opensearch.action.search.SearchResponse response = org.opensearch.action.search.SearchResponse.fromXContent(parser);
        assertTrue(response.getAggregations().asMap().containsKey("unique_connector_names"));
    }

    private XContentParser createParser(XContentBuilder builder) throws IOException {
        return SdkClientUtils.createParser(builder.toString());
    }
}
