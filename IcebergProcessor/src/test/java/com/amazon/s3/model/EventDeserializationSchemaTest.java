/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: MIT-0
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy of this
 * software and associated documentation files (the "Software"), to deal in the Software
 * without restriction, including without limitation the rights to use, copy, modify,
 * merge, publish, distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED,
 * INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A
 * PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
 * HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
 * OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
 * SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 */
package com.amazon.s3.model;

import com.amazon.s3.TestingHelpers;
import org.apache.flink.api.common.functions.util.ListCollector;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.junit.jupiter.api.Test;

import software.amazon.awssdk.services.dynamodb.model.Record;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class EventDeserializationSchemaTest {

    @Test
    public void testDeserialization() throws Exception {
        EventDeserializationSchema deserializationSchema = new EventDeserializationSchema();

        Record record = TestingHelpers.loadSampleEventAsRecord("CreateEvent");

        List<ImageRecord> results = new ArrayList<>();
        deserializationSchema.deserialize(record, "TestStream", "TestShard", new ListCollector<>(results));
        assertEquals(1, results.size());

        ImageRecord deserialized = results.get(0);
        assertEquals("foo", deserialized.bucket);
    }

    @Test
    public void testProducedTypeOfDeserializer() throws Exception {
        EventDeserializationSchema deserializationSchema = new EventDeserializationSchema();
        assertEquals(TypeInformation.of(ImageRecord.class), deserializationSchema.getProducedType());
    }
}