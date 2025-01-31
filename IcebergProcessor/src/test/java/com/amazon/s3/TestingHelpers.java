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
package com.amazon.s3;

import com.amazon.s3.model.ImageRecord;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.Record;

import java.time.Instant;
import java.util.*;

public class TestingHelpers {
    private final static Map<String, Record> TEST_RECORDS = Map.of(
            "CreateEvent",
            Record.builder()
                    .eventID("cc92a2c2201d3b8f576460e272df8085")
                    .eventName("MODIFY")
                    .eventVersion("1.1")
                    .eventSource("aws:dynamodb")
                    .awsRegion("us-east-1")
                    .dynamodb(builder -> builder
                        .approximateCreationDateTime(Instant.ofEpochMilli(1731675048000L))
                        .keys(Map.of(
                            "s3key", AttributeValue.fromS("czNjb250ZW50bWV0YWRhdGFleGFtcGxlc3RhY2staW5wdXRkNjBjZWY3YS00dnNrOTQ2ZXZ6aGEvMjAyNDA3MjRfMTIzMTA3LmpwZw==-0")
                        ))
                        .newImage(Map.of(
                                "bucket", AttributeValue.fromS("foo"),
                                "metadata", AttributeValue.fromS("{\"labels\": [{\"Name\": \"Pond\", \"Confidence\": 96.9462661743164, \"Instances\": [], \"Parents\": [{\"Name\": \"Nature\"}, {\"Name\": \"Outdoors\"}, {\"Name\": \"Water\"}], \"Aliases\": [], \"Categories\": [{\"Name\": \"Nature and Outdoors\"}]}, {\"Name\": \"Bird\", \"Confidence\": 96.28910827636719, \"Instances\": [{\"BoundingBox\": {\"Width\": 0.22996997833251953, \"Height\": 0.18747131526470184, \"Left\": 0.3471854031085968, \"Top\": 0.4301318824291229}, \"Confidence\": 95.57042694091797}, {\"BoundingBox\": {\"Width\": 0.28269097208976746, \"Height\": 0.130358025431633, \"Left\": 0.18622398376464844, \"Top\": 0.39367493987083435}, \"Confidence\": 95.50381469726562}, {\"BoundingBox\": {\"Width\": 0.3787681460380554, \"Height\": 0.18510495126247406, \"Left\": 0.005001159384846687, \"Top\": 0.5012580752372742}, \"Confidence\": 93.73571014404297}, {\"BoundingBox\": {\"Width\": 0.19017085433006287, \"Height\": 0.1664065271615982, \"Left\": 0.5836058855056763, \"Top\": 0.3484640121459961}, \"Confidence\": 91.99315643310547}], \"Parents\": [{\"Name\": \"Animal\"}], \"Aliases\": [], \"Categories\": [{\"Name\": \"Animals and Pets\"}]}, {\"Name\": \"Waterfowl\", \"Confidence\": 96.28910827636719, \"Instances\": [], \"Parents\": [{\"Name\": \"Animal\"}, {\"Name\": \"Bird\"}], \"Aliases\": [], \"Categories\": [{\"Name\": \"Animals and Pets\"}]}], \"exif\": {\"ImageWidth\": 4000, \"ImageLength\": 2252, \"ResolutionUnit\": 2, \"ExifOffset\": 226, \"Make\": \"samsung\", \"Model\": \"SM-G998B\", \"Software\": \"G998BXXSCGXF5\", \"Orientation\": 6, \"DateTime\": \"2024:07:24 12:31:07\", \"YCbCrPositioning\": 1, \"XResolution\": {\"type\": \"IFDRational\", \"denominator\": 1, \"imag\": 0, \"numerator\": 72}, \"YResolution\": {\"type\": \"IFDRational\", \"denominator\": 1, \"imag\": 0, \"numerator\": 72}, \"ExifVersion\": \"MDIyMA==\", \"ShutterSpeedValue\": {\"type\": \"IFDRational\", \"denominator\": 204, \"imag\": 0, \"numerator\": 1}, \"ApertureValue\": {\"type\": \"IFDRational\", \"denominator\": 100, \"imag\": 0, \"numerator\": 169}, \"DateTimeOriginal\": \"2024:07:24 12:31:07\", \"DateTimeDigitized\": \"2024:07:24 12:31:07\", \"BrightnessValue\": {\"type\": \"IFDRational\", \"denominator\": 100, \"imag\": 0, \"numerator\": 686}, \"ExposureBiasValue\": {\"type\": \"IFDRational\", \"denominator\": 100, \"imag\": 0, \"numerator\": 0}, \"MaxApertureValue\": {\"type\": \"IFDRational\", \"denominator\": 100, \"imag\": 0, \"numerator\": 169}, \"MeteringMode\": 2, \"Flash\": 0, \"FocalLength\": {\"type\": \"IFDRational\", \"denominator\": 100, \"imag\": 0, \"numerator\": 670}, \"ColorSpace\": 1, \"ExifImageWidth\": 4000, \"DigitalZoomRatio\": {\"type\": \"IFDRational\", \"denominator\": 100, \"imag\": 0, \"numerator\": 100}, \"FocalLengthIn35mmFilm\": 24, \"SceneCaptureType\": 0, \"OffsetTime\": \"+01:00\", \"OffsetTimeOriginal\": \"+01:00\", \"SubsecTime\": \"297\", \"SubsecTimeOriginal\": \"297\", \"SubsecTimeDigitized\": \"297\", \"ExifImageHeight\": 2252, \"ExposureTime\": {\"type\": \"IFDRational\", \"denominator\": 204, \"imag\": 0, \"numerator\": 1}, \"FNumber\": {\"type\": \"IFDRational\", \"denominator\": 100, \"imag\": 0, \"numerator\": 180}, \"ImageUniqueID\": \"XA8XLNF00SM\", \"ExposureProgram\": 2, \"ISOSpeedRatings\": 50, \"ExposureMode\": 0, \"FlashPixVersion\": \"MDEwMA==\", \"WhiteBalance\": 0}}"),
                                "s3key", AttributeValue.fromS("czNjb250ZW50bWV0YWRhdGFleGFtcGxlc3RhY2staW5wdXRkNjBjZWY3YS00dnNrOTQ2ZXZ6aGEvMjAyNDA3MjRfMTIzMTA3LmpwZw==-0"),
                                "etag", AttributeValue.fromS("86cfe4562a912649058b0fb7824e1d11"),
                                "latest_event_time", AttributeValue.fromS("2024-11-15T12:50:40+00:00"),
                                "version_id", AttributeValue.fromNul(true),
                                "key", AttributeValue.fromS("20240724_123107.jpg"),
                                "sequencer", AttributeValue.fromS("00673743A054CE73CC")
                        ))
                        .sequenceNumber("14055600003598882686763475")
                        .sizeBytes(3437L)
                        .streamViewType("NEW_IMAGE")
                    )
                    .build(),
            "CreateEvent_AllMetadataTypes",
            Record.builder()
                    .eventID("cc92a2c2201d3b8f576460e272df8085")
                    .eventName("MODIFY")
                    .eventVersion("1.1")
                    .eventSource("aws:dynamodb")
                    .awsRegion("us-east-1")
                    .dynamodb(builder -> builder
                            .approximateCreationDateTime(Instant.ofEpochMilli(1731675048000L))
                            .keys(Map.of(
                                    "s3key", AttributeValue.fromS("czNjb250ZW50bWV0YWRhdGFleGFtcGxlc3RhY2staW5wdXRkNjBjZWY3YS00dnNrOTQ2ZXZ6aGEvMjAyNDA3MjRfMTIzMTA3LmpwZw==-0")
                            ))
                            .newImage(Map.of(
                                    "bucket", AttributeValue.fromS("foo"),
                                    "metadata", AttributeValue.fromS("{\"metadata\":{\"array\":{\"string\":[\"foo\",\"bar\",\"baz\"],\"integer\":[12,13,14],\"boolean\":[true,true,false]},\"scalar\":{\"string\":\"foobar\",\"integer\":12,\"boolean\":true}}}"),
                                    "s3key", AttributeValue.fromS("czNjb250ZW50bWV0YWRhdGFleGFtcGxlc3RhY2staW5wdXRkNjBjZWY3YS00dnNrOTQ2ZXZ6aGEvMjAyNDA3MjRfMTIzMTA3LmpwZw==-0"),
                                    "etag", AttributeValue.fromS("86cfe4562a912649058b0fb7824e1d11"),
                                    "latest_event_time", AttributeValue.fromS("2024-11-15T12:50:40+00:00"),
                                    "version_id", AttributeValue.fromNul(true),
                                    "key", AttributeValue.fromS("20240724_123107.jpg"),
                                    "sequencer", AttributeValue.fromS("00673743A054CE73CC")
                            ))
                            .sequenceNumber("14055600003598882686763475")
                            .sizeBytes(3437L)
                            .streamViewType("NEW_IMAGE")
                    )
                    .build(),
            "DeleteEvent",
            Record.builder()
                    .eventID("63022c1585fd0be107b7cfd9e39391d7")
                    .eventName("MODIFY")
                    .eventVersion("1.1")
                    .eventSource("aws:dynamodb")
                    .awsRegion("us-east-1")
                    .dynamodb(builder -> builder
                            .approximateCreationDateTime(Instant.ofEpochMilli(1731925977000L))
                            .keys(Map.of(
                                    "s3key", AttributeValue.fromS("czNjb250ZW50bWV0YWRhdGFleGFtcGxlc3RhY2staW5wdXRkNjBjZWY3YS00dnNrOTQ2ZXZ6aGEvd2FyZWhvdXNlL2RhdGEvMDAwMDAtMC05ZTc1MTZlOS03NWZkLTRlZTgtODliOS0wY2RhOGJiMzIwYWQtMDAwMDgucGFycXVldA==-0")
                            ))
                            .newImage(Map.of(
                                    "bucket", AttributeValue.fromS("foo"),
                                    "s3key", AttributeValue.fromS("czNjb250ZW50bWV0YWRhdGFleGFtcGxlc3RhY2staW5wdXRkNjBjZWY3YS00dnNrOTQ2ZXZ6aGEvd2FyZWhvdXNlL2RhdGEvMDAwMDAtMC05ZTc1MTZlOS03NWZkLTRlZTgtODliOS0wY2RhOGJiMzIwYWQtMDAwMDgucGFycXVldA==-0"),
                                    "deleted", AttributeValue.fromBool(true),
                                    "latest_event_time", AttributeValue.fromS("2024-11-15T12:50:40+00:00"),
                                    "version_id", AttributeValue.fromNul(true),
                                    "key", AttributeValue.fromS("20240724_123107.jpg"),
                                    "sequencer", AttributeValue.fromS("00673B17D652EE0D14")
                            ))
                            .sequenceNumber("27630900001741775691079632")
                            .sizeBytes(592L)
                            .streamViewType("NEW_IMAGE")
                    )
                    .build()
    );

    public static Map<String, Properties> defaultCorrectProperties() {
        Map<String, Properties> properties = new HashMap<>();
        properties.put("sdk", new Properties());
        properties.get("sdk").setProperty("region", "us-east-1");

        properties.put("stream", new Properties());
        properties.get("stream").setProperty("arn", "arn:aws:dynamodb:us-east-1:111222333444:table/SomeTableStreamArn");

        properties.put("schema", new Properties());
        properties.get("schema").setProperty("include_raw_metadata", "true");

        properties.put("catalog", new Properties());
        properties.get("catalog").setProperty("name", "S3");
        properties.get("catalog").setProperty("database", "default");
        properties.get("catalog").setProperty("table", "s3_content_metadata");
        properties.get("catalog").setProperty("warehousePath", "s3://example/warehouse");
        properties.get("catalog").setProperty("impl", "org.apache.iceberg.aws.glue.GlueCatalog");

        return properties;
    }

    public static Record loadSampleEventAsRecord(String eventName) {
        return TEST_RECORDS.get(eventName);
    }

    public static ImageRecord loadSampleEventAsImageRecord(String eventName) {
        return new ImageRecord(loadSampleEventAsRecord(eventName).dynamodb().newImage());
    }
}
