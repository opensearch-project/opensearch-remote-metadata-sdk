/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.remote.metadata.common;

import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.opensearch.core.xcontent.XContentParserUtils.ensureExpectedToken;

public class ComplexDataObject implements ToXContentObject {
    private String testString;
    private long testNumber;
    private boolean testBool;
    private List<String> testList;
    private TestDataObject testObject;

    public ComplexDataObject() {}

    public ComplexDataObject(String testString, long testNumber, boolean testBool, List<String> testList, TestDataObject testObject) {
        this.testString = testString;
        this.testNumber = testNumber;
        this.testBool = testBool;
        this.testList = testList;
        this.testObject = testObject;
    }

    public String getTestString() {
        return testString;
    }

    public long getTestNumber() {
        return testNumber;
    }

    public boolean isTestBool() {
        return testBool;
    }

    public List<String> getTestList() {
        return testList;
    }

    public TestDataObject getTestObject() {
        return testObject;
    }

    public void setTestString(String testString) {
        this.testString = testString;
    }

    public void setTestNumber(long testNumber) {
        this.testNumber = testNumber;
    }

    public void setTestBool(boolean testBool) {
        this.testBool = testBool;
    }

    public void setTestList(List<String> testList) {
        this.testList = testList;
    }

    public void setTestObject(TestDataObject testObject) {
        this.testObject = testObject;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        XContentBuilder xContentBuilder = builder.startObject();
        xContentBuilder.field("testString", this.testString);
        xContentBuilder.field("testNumber", this.testNumber);
        xContentBuilder.field("testBool", this.testBool);
        xContentBuilder.field("testList", this.testList);
        xContentBuilder.field("testObject", this.testObject);
        return xContentBuilder.endObject();
    }

    public static ComplexDataObject parse(XContentParser parser) throws IOException {
        ComplexDataObject obj = new ComplexDataObject();
        ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.currentToken(), parser);
        while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
            String fieldName = parser.currentName();
            parser.nextToken();
            if ("testString".equals(fieldName)) {
                obj.setTestString(parser.text());
            } else if ("testNumber".equals(fieldName)) {
                obj.setTestNumber(parser.longValue());
            } else if ("testBool".equals(fieldName)) {
                obj.setTestBool(parser.booleanValue());
            } else if ("testList".equals(fieldName)) {
                ensureExpectedToken(XContentParser.Token.START_ARRAY, parser.currentToken(), parser);
                List<String> list = new ArrayList<>();
                while (parser.nextToken() != XContentParser.Token.END_ARRAY) {
                    list.add(parser.text());
                }
                obj.setTestList(list);
            } else if ("testObject".equals(fieldName)) {
                obj.setTestObject(TestDataObject.parse(parser));
            }
        }
        return obj;
    }

    // Builder class
    public static class Builder {
        private String testString;
        private long testNumber;
        private boolean testBool;
        private List<String> testList;
        private TestDataObject testObject;

        public Builder testString(String testString) {
            this.testString = testString;
            return this;
        }

        public Builder testNumber(long testNumber) {
            this.testNumber = testNumber;
            return this;
        }

        public Builder testBool(boolean testBool) {
            this.testBool = testBool;
            return this;
        }

        public Builder testList(List<String> testList) {
            this.testList = testList;
            return this;
        }

        public Builder testObject(TestDataObject testObject) {
            this.testObject = testObject;
            return this;
        }

        public ComplexDataObject build() {
            return new ComplexDataObject(testString, testNumber, testBool, testList, testObject);
        }
    }

    public static Builder builder() {
        return new Builder();
    }
}
