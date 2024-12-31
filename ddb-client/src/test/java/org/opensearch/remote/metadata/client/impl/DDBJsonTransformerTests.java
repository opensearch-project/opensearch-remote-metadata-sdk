/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.remote.metadata.client.impl;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

import org.opensearch.core.common.Strings;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.remote.metadata.common.ComplexDataObject;
import org.opensearch.remote.metadata.common.TestDataObject;
import org.opensearch.test.OpenSearchTestCase;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class DDBJsonTransformerTests extends OpenSearchTestCase {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    @Test
    public void convertJsonObjectToItem_HappyCase() throws Exception {
        TestDataObject testDataObject = new TestDataObject("foo");

        ComplexDataObject complexDataObject = ComplexDataObject.builder()
            .testString("testString")
            .testNumber(123)
            .testBool(true)
            .testList(Arrays.asList("123", "hello", null))
            .testObject(testDataObject)
            .build();
        String source = Strings.toString(MediaTypeRegistry.JSON, complexDataObject);
        Map<String, AttributeValue> itemResponse = DDBJsonTransformer.convertJsonObjectToDDBAttributeMap(OBJECT_MAPPER.readTree(source));
        Assert.assertEquals(5, itemResponse.size());
        Assert.assertEquals("testString", itemResponse.get("testString").s());
        Assert.assertEquals("123", itemResponse.get("testNumber").n());
        Assert.assertEquals(true, itemResponse.get("testBool").bool());
        Assert.assertEquals("foo", itemResponse.get("testObject").m().get("data").s());
        Assert.assertEquals("123", itemResponse.get("testList").l().get(0).s());
        Assert.assertEquals("hello", itemResponse.get("testList").l().get(1).s());
    }

    @Test
    public void convertJsonArrayToAttributeValueListTest() {
        ArrayNode arrayNode = OBJECT_MAPPER.createArrayNode();
        arrayNode.add(10);
        arrayNode.add("test");
        arrayNode.add(true);
        arrayNode.add((String) null);
        arrayNode.add(OBJECT_MAPPER.createArrayNode());
        arrayNode.add(OBJECT_MAPPER.createObjectNode());
        List<AttributeValue> attributeValueList = DDBJsonTransformer.convertJsonArrayToAttributeValueList(arrayNode);
        Assert.assertEquals(6, attributeValueList.size());
    }

    @Test
    public void convertJsonArrayToAttributeValueList_TestMultipleJsonType() throws Exception {
        ObjectMapper objectMapper = new ObjectMapper();

        JsonNode jsonNode = objectMapper.readTree("[\"testString\", 123, true, [\"test1\", \"test2\"], {\"hello\": \"all\"}]");
        List<AttributeValue> list = DDBJsonTransformer.convertJsonArrayToAttributeValueList(jsonNode);
        assertEquals(5, list.size());
    }

    @Test
    public void convertToObjectNode_TestNullInput() {
        AttributeValue nullAttribute = AttributeValue.builder().nul(true).build();
        ObjectNode response = DDBJsonTransformer.convertDDBAttributeValueMapToObjectNode(Map.of("test", nullAttribute));
        Assert.assertTrue(response.get("test").isNull());
    }

    @Test
    public void convertToObjectNode_TestInvalidInput() {
        AttributeValue nsAttribute = AttributeValue.builder().ns("123").build();
        Assert.assertThrows(
            IllegalArgumentException.class,
            () -> DDBJsonTransformer.convertDDBAttributeValueMapToObjectNode(Map.of("test", nsAttribute))
        );
    }

    @Test
    public void convertToArrayNode_MultipleDataTypes() {
        ArrayNode arrayNode = DDBJsonTransformer.convertAttributeValueListToArrayNode(
            Arrays.asList(
                AttributeValue.builder().s("test").build(),
                AttributeValue.builder().n("123").build(),
                AttributeValue.builder().l(AttributeValue.builder().s("testList").build()).build(),
                AttributeValue.builder().nul(true).build(),
                AttributeValue.builder().m(Map.of("key", AttributeValue.builder().s("testMap").build())).build()
            )
        );
        assertEquals(5, arrayNode.size());
    }
}
