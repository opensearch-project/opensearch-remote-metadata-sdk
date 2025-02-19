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
import org.opensearch.action.support.PlainActionFuture;
import org.opensearch.core.rest.RestStatus;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletionException;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class SdkClientUtilsTests {

    private OpenSearchStatusException testException;
    private InterruptedException interruptedException;
    private IOException ioException;

    @BeforeEach
    public void setUp() {
        testException = new OpenSearchStatusException("Test", RestStatus.BAD_REQUEST);
        interruptedException = new InterruptedException();
        ioException = new IOException();
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
}
