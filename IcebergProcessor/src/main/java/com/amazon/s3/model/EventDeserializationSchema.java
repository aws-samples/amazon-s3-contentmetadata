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

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.dynamodb.source.serialization.DynamoDbStreamsDeserializationSchema;
import org.apache.flink.util.Collector;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import software.amazon.awssdk.services.dynamodb.model.OperationType;
import software.amazon.awssdk.services.dynamodb.model.Record;

import java.io.IOException;

public class EventDeserializationSchema implements DynamoDbStreamsDeserializationSchema<ImageRecord> {
    private static final Logger LOG = LogManager.getLogger(EventDeserializationSchema.class);
    private static final long serialVersionUID = 1L;

    @Override
    public void deserialize(Record record, String stream, String shardId, Collector<ImageRecord> output) throws IOException {
        LOG.trace(record);

        // Elements bring removed from the DDB table don't have a semantic value, so we ignore them here.
        if(record.eventName() == OperationType.REMOVE) {
            return;
        }
        output.collect(new ImageRecord(record.dynamodb().newImage()));
    }

    @Override
    public TypeInformation<ImageRecord> getProducedType() {
        return TypeInformation.of(ImageRecord.class);
    }
}
