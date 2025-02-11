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
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.action.delete.DeleteResponse;
import org.opensearch.action.get.GetResponse;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.update.UpdateResponse;
import org.opensearch.common.action.ActionFuture;
import org.opensearch.common.util.concurrent.FutureUtils;
import org.opensearch.common.util.concurrent.UncategorizedExecutionException;
import org.opensearch.core.action.ActionListener;
import org.opensearch.remote.metadata.client.BulkDataObjectResponse;
import org.opensearch.remote.metadata.client.DeleteDataObjectResponse;
import org.opensearch.remote.metadata.client.GetDataObjectResponse;
import org.opensearch.remote.metadata.client.PutDataObjectResponse;
import org.opensearch.remote.metadata.client.SearchDataObjectResponse;
import org.opensearch.remote.metadata.client.UpdateDataObjectResponse;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.function.BiConsumer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.opensearch.core.rest.RestStatus.INTERNAL_SERVER_ERROR;

/**
 * Utility methods for client implementations
 */
public class SdkClientUtils {

    private SdkClientUtils() {}

    /**
     * Wraps the completion of a PUT operation from the SdkClient into a format compatible with an ActionListener.
     *
     * @param listener The ActionListener that will receive the parsed IndexResponse or any errors
     * @return A BiConsumer that can be used directly with CompletionStage's whenComplete method
     */
    public static BiConsumer<PutDataObjectResponse, Throwable> wrapPutCompletion(ActionListener<IndexResponse> listener) {
        return (r, throwable) -> {
            if (throwable == null) {
                try {
                    IndexResponse indexResponse = r.parser() == null ? null : IndexResponse.fromXContent(r.parser());
                    listener.onResponse(indexResponse);
                } catch (IOException e) {
                    handleParseFailure(listener, "put");
                }
            } else {
                handleThrowable(listener, throwable);
            }
        };
    }

    /**
     * Wraps the completion of a GET operation from the SdkClient into a format compatible with an ActionListener.
     *
     * @param listener The ActionListener that will receive the parsed GetResponse or any errors
     * @return A BiConsumer that can be used directly with CompletionStage's whenComplete method
     */
    public static BiConsumer<GetDataObjectResponse, Throwable> wrapGetCompletion(ActionListener<GetResponse> listener) {
        return (r, throwable) -> {
            if (throwable == null) {
                try {
                    GetResponse getResponse = r.parser() == null ? null : GetResponse.fromXContent(r.parser());
                    listener.onResponse(getResponse);
                } catch (IOException e) {
                    handleParseFailure(listener, "get");
                }
            } else {
                handleThrowable(listener, throwable);
            }
        };
    }

    /**
     * Wraps the completion of an UPDATE operation from the SdkClient into a format compatible with an ActionListener.
     *
     * @param listener The ActionListener that will receive the parsed UpdateResponse or any errors
     * @return A BiConsumer that can be used directly with CompletionStage's whenComplete method
     */
    public static BiConsumer<UpdateDataObjectResponse, Throwable> wrapUpdateCompletion(ActionListener<UpdateResponse> listener) {
        return (r, throwable) -> {
            if (throwable == null) {
                try {
                    UpdateResponse updateResponse = r.parser() == null ? null : UpdateResponse.fromXContent(r.parser());
                    listener.onResponse(updateResponse);
                } catch (IOException e) {
                    handleParseFailure(listener, "update");
                }
            } else {
                handleThrowable(listener, throwable);
            }
        };
    }

    /**
     * Wraps the completion of a DELETE operation from the SdkClient into a format compatible with an ActionListener.
     *
     * @param listener The ActionListener that will receive the parsed DeleteResponse or any errors
     * @return A BiConsumer that can be used directly with CompletionStage's whenComplete method
     */
    public static BiConsumer<DeleteDataObjectResponse, Throwable> wrapDeleteCompletion(ActionListener<DeleteResponse> listener) {
        return (r, throwable) -> {
            if (throwable == null) {
                try {
                    DeleteResponse deleteResponse = r.parser() == null ? null : DeleteResponse.fromXContent(r.parser());
                    listener.onResponse(deleteResponse);
                } catch (IOException e) {
                    handleParseFailure(listener, "delete");
                }
            } else {
                handleThrowable(listener, throwable);
            }
        };
    }

    /**
     * Wraps the completion of a BULK operation from the SdkClient into a format compatible with an ActionListener.
     *
     * @param listener The ActionListener that will receive the parsed BulkResponse or any errors
     * @return A BiConsumer that can be used directly with CompletionStage's whenComplete method
     */
    public static BiConsumer<BulkDataObjectResponse, Throwable> wrapBulkCompletion(ActionListener<BulkResponse> listener) {
        return (r, throwable) -> {
            if (throwable == null) {
                try {
                    BulkResponse bulkResponse = r.parser() == null ? null : BulkResponse.fromXContent(r.parser());
                    listener.onResponse(bulkResponse);
                } catch (IOException e) {
                    handleParseFailure(listener, "bulk");
                }
            } else {
                handleThrowable(listener, throwable);
            }
        };
    }

    /**
     * Wraps the completion of a SEARCH operation from the SdkClient into a format compatible with an ActionListener.
     *
     * @param listener The ActionListener that will receive the parsed SearchResponse or any errors
     * @return A BiConsumer that can be used directly with CompletionStage's whenComplete method
     */
    public static BiConsumer<SearchDataObjectResponse, Throwable> wrapSearchCompletion(ActionListener<SearchResponse> listener) {
        return (r, throwable) -> {
            if (throwable == null) {
                try {
                    SearchResponse searchResponse = r.parser() == null ? null : SearchResponse.fromXContent(r.parser());
                    listener.onResponse(searchResponse);
                } catch (IOException e) {
                    handleParseFailure(listener, "search");
                }
            } else {
                handleThrowable(listener, throwable);
            }
        };
    }

    private static void handleParseFailure(ActionListener<?> listener, String operation) {
        listener.onFailure(new OpenSearchStatusException("Failed to parse " + operation + " response", INTERNAL_SERVER_ERROR));
    }

    private static void handleThrowable(ActionListener<?> listener, Throwable throwable) {
        Exception exception = unwrapAndConvertToException(throwable, CompletionException.class, OpenSearchStatusException.class);
        listener.onFailure(exception);
    }

    /**
     * Unwraps the cause of a {@link CompletionException}. If the cause is an {@link Exception}, rethrows the exception.
     * Otherwise wraps it in an {@link OpenSearchException}. Properly re-interrupts the thread on {@link InterruptedException}.
     * @param throwable a throwable.
     * @param exceptionTypesToUnwrap optional list of exception types to unwrap. Defaults to {@link CompletionException}.
     * @return the cause of the completion exception or the throwable, directly if an {@link Exception} or wrapped in an OpenSearchException otherwise.
     */
    @SafeVarargs
    public static Exception unwrapAndConvertToException(Throwable throwable, Class<? extends Throwable>... exceptionTypesToUnwrap) {
        // Unwrap specified exception types or pass through other exceptions
        List<Class<? extends Throwable>> unwrapTypes = (exceptionTypesToUnwrap.length > 0)
            ? Arrays.asList(exceptionTypesToUnwrap)
            : List.of(CompletionException.class);

        Throwable cause = throwable;
        while (cause != null && unwrapTypes.contains(cause.getClass()) && cause.getCause() != null) {
            cause = cause.getCause();
        }

        // Double-unwrap checked exceptions wrapped in ExecutionException
        cause = getRethrownExecutionExceptionRootCause(cause);
        if (cause instanceof InterruptedException) {
            Thread.currentThread().interrupt();
        }
        if (cause instanceof Exception) {
            return (Exception) cause;
        }
        return new OpenSearchException(cause);
    }

    /**
     * Get the original exception of an {@link UncategorizedExecutionException} with two levels of cause nesting.
     * Intended to recreate the root cause of an exception thrown by {@link ActionFuture#actionGet}, which was handled by {@link FutureUtils#rethrowExecutionException}.
     * @param throwable a throwable with possibly nested causes
     * @return the root cause of an ExecutionException if it was not a RuntimeException, otherwise the original exception
     */
    public static Throwable getRethrownExecutionExceptionRootCause(Throwable throwable) {
        if (throwable instanceof UncategorizedExecutionException && throwable.getCause() instanceof ExecutionException) {
            return throwable.getCause().getCause();
        }
        return throwable;
    }

    /**
     * If an internal variable is an enum represented by all upper case, the Remote client may have it mapped in lower case. This method lowercases these enum values
     * @param field The JSON field to lowercase the value
     * @param json The full JSON to process
     * @return The JSON with the value lowercased
     */
    public static String lowerCaseEnumValues(String field, String json) {
        // Use a matcher to find and replace the field value in lowercase
        Matcher matcher = Pattern.compile("(\"" + Pattern.quote(field) + "\"):(\"[A-Z_]+\")").matcher(json);
        StringBuffer sb = new StringBuffer();
        while (matcher.find()) {
            matcher.appendReplacement(sb, matcher.group(1) + ":" + matcher.group(2).toLowerCase(Locale.ROOT));
        }
        matcher.appendTail(sb);
        return sb.toString();
    }
}
