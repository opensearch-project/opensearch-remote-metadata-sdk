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
import org.opensearch.client.json.JsonpMapper;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.remote.metadata.client.PutDataObjectRequest;
import org.opensearch.remote.metadata.client.impl.JsonTransformer.XContentObjectJsonpSerializer;
import org.opensearch.test.OpenSearchTestCase;
import org.junit.Test;

import java.io.IOException;
import java.io.StringReader;
import java.io.StringWriter;
import java.util.Map;

import jakarta.json.Json;
import jakarta.json.JsonObject;
import jakarta.json.stream.JsonGenerator;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class JsonTransformerTests extends OpenSearchTestCase {

    private XContentObjectJsonpSerializer xContentSerializer = new XContentObjectJsonpSerializer();
    private JsonpMapper jsonpMapper = mock(JsonpMapper.class);

    @Test
    public void testSerialize_HappyPath() {
        PutDataObjectRequest putRequest = PutDataObjectRequest.builder().dataObject(Map.of("foo", Map.of("bar", "baz"))).build();
        ToXContentObject dataObject = putRequest.dataObject();
        StringWriter stringWriter = new StringWriter();
        try (JsonGenerator jsonGenerator = Json.createGenerator(stringWriter)) {
            xContentSerializer.serialize(dataObject, jsonGenerator, jsonpMapper);
            jsonGenerator.flush();
        }
        assertEquals("{\"foo\":{\"bar\":\"baz\"}}", stringWriter.toString());

        JsonObject jsonObject = Json.createReader(new StringReader(stringWriter.toString())).readObject();
        assertEquals("baz", jsonObject.getJsonObject("foo").getString("bar"));
    }

    @Test
    public void testSerialize_IllegalArgumentPath() {
        Map<String, String> notAnXContentObject = Map.of("foo", "bar");
        JsonGenerator jsonGenerator = mock(JsonGenerator.class);
        IllegalArgumentException ex = assertThrows(
            IllegalArgumentException.class,
            () -> xContentSerializer.serialize(notAnXContentObject, jsonGenerator, jsonpMapper)
        );
        assertTrue(ex.getMessage().contains("This method requires an object of type ToXContentObject"));
    }

    @Test
    public void testSerialize_ParseExceptionPath() throws IOException {
        ToXContentObject invalidXContentObject = mock(ToXContentObject.class);
        when(invalidXContentObject.toXContent(any(), any())).thenThrow(new IOException("X"));
        JsonGenerator jsonGenerator = mock(JsonGenerator.class);
        OpenSearchStatusException ex = assertThrows(
            OpenSearchStatusException.class,
            () -> xContentSerializer.serialize(invalidXContentObject, jsonGenerator, jsonpMapper)
        );
        assertEquals(RestStatus.BAD_REQUEST, ex.status());
    }
}
