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
import org.opensearch.client.json.JsonpSerializer;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;
import java.io.StringReader;

import jakarta.json.Json;
import jakarta.json.JsonReader;
import jakarta.json.stream.JsonGenerator;

/**
 * Utility methods for transforming JSON to objects
 */
public class JsonTransformer {

    private JsonTransformer() {}

    /**
     * A JsonPSerializer serializing XContent
     */
    public static class XContentObjectJsonpSerializer implements JsonpSerializer<Object> {
        @Override
        public void serialize(Object obj, JsonGenerator generator, JsonpMapper mapper) {
            if (obj instanceof ToXContentObject) {
                serialize((ToXContentObject) obj, generator);
            } else {
                throw new IllegalArgumentException(
                    "This method requires an object of type ToXContentObject, actual type is " + obj.getClass().getName()
                );
            }
        }

        private void serialize(ToXContentObject obj, JsonGenerator generator) {
            try (XContentBuilder builder = XContentFactory.jsonBuilder()) {
                obj.toXContent(builder, ToXContent.EMPTY_PARAMS);
                serializeString(builder.toString(), generator);
            } catch (IOException e) {
                throw new OpenSearchStatusException("Error parsing XContentObject", RestStatus.BAD_REQUEST);
            }
        }

        private void serializeString(String json, JsonGenerator generator) {
            try (JsonReader jsonReader = Json.createReader(new StringReader(json))) {
                generator.write(jsonReader.readObject());
            }
        }
    }
}
