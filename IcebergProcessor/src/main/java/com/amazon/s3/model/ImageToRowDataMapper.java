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

import com.amazon.s3.schema.TableSchemaGenerator;
import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.DocumentContext;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.Option;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.data.*;
import org.apache.flink.table.types.AtomicDataType;
import org.apache.flink.table.types.CollectionDataType;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.types.RowKind;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Manages the translation from a ChangeEvent into a Row as understood by Flink's Table APIs.
 */
public class ImageToRowDataMapper implements MapFunction<ImageRecord, RowData>, ResultTypeQueryable<RowData> {

    private final List<TableSchemaGenerator.SchemaEntry> schemaEntries;

    public ImageToRowDataMapper(Schema tableSchema, List<TableSchemaGenerator.SchemaEntry> customSchemaElements) {
        Map<String, TableSchemaGenerator.SchemaEntry> customEntryMap =
                customSchemaElements.stream().collect(Collectors.toMap((x) -> x.name, (x) -> x));
        schemaEntries = tableSchema
                .getColumns()
                .stream()
                .map(x -> (Schema.UnresolvedPhysicalColumn)x)
                .map(col ->
                        new TableSchemaGenerator.SchemaEntry(
                            col.getName(),
                                (DataType)col.getDataType(),
                            customEntryMap.containsKey(col.getName()) ? customEntryMap.get(col.getName()).jpath : null
                        ))
                .collect(Collectors.toList());
    }

    @Override
    public RowData map(ImageRecord image) {
        GenericRowData row;
        DocumentContext parsedMetadata = null;
        if (image.isDelete || image.isDeleteMarker) {
            row = new GenericRowData(RowKind.DELETE, schemaEntries.size());
        } else {
            row = new GenericRowData(RowKind.INSERT, schemaEntries.size());
            parsedMetadata = JsonPath.parse(
                    image.metadata,
                    Configuration.defaultConfiguration().addOptions(Option.SUPPRESS_EXCEPTIONS));
        }

        for (int fieldIndex = 0; fieldIndex < schemaEntries.size(); fieldIndex++) {
            TableSchemaGenerator.SchemaEntry column = schemaEntries.get(fieldIndex);

            switch (column.name) {
                case "bucket": row.setField(fieldIndex, StringData.fromString(image.bucket)); break;
                case "key": row.setField(fieldIndex, StringData.fromString(image.userKey)); break;
                case "versionId": row.setField(fieldIndex, StringData.fromString(image.versionId)); break;
                case "sequencer": row.setField(fieldIndex, StringData.fromString(image.sequencer)); break;
                case "etag": row.setField(fieldIndex, StringData.fromString(image.etag)); break;
                case "metadata": row.setField(fieldIndex, StringData.fromString(image.metadata)); break;
                case "lastModified": row.setField(fieldIndex, TimestampData.fromLocalDateTime(LocalDateTime.parse(image.latestEventTime, DateTimeFormatter.ISO_DATE_TIME))); break;
                default: row.setField(fieldIndex, extractColumnValue(column, parsedMetadata)); break;
            }
        }
        return row;
    }

    /*
        Clearly this approach is broken.  Need to do the hard bit eventually...
     */
    private static Object extractColumnValue(TableSchemaGenerator.SchemaEntry column, DocumentContext metadataJson) {
        if (metadataJson == null) {
            return null;
        }

        if (column.type instanceof AtomicDataType) {
            if(column.type.getLogicalType().supportsOutputConversion(Integer.class)) {
                return metadataJson.<Integer>read(column.jpath);
            } else if (column.type.getLogicalType().supportsOutputConversion(Boolean.class)) {
                return metadataJson.<Boolean>read(column.jpath);
            } else if (column.type.getLogicalType().supportsOutputConversion(String.class)) {
                return StringData.fromString(metadataJson.<String>read(column.jpath));
            } else {
                throw new RuntimeException("Unsupported column type: " + column.type);
            }
        } else if (column.type instanceof CollectionDataType) {
            LogicalType logicalType = ((CollectionDataType) column.type).getElementDataType().getLogicalType();
            if(logicalType.supportsOutputConversion(Integer.class)) {
                return new GenericArrayData(metadataJson.<List<Integer>>read(column.jpath).toArray());
            } else if (logicalType.supportsOutputConversion(Boolean.class)) {
                return new GenericArrayData(metadataJson.<List<Boolean>>read(column.jpath).toArray());
            } else if (logicalType.supportsOutputConversion(String.class)) {
                return new GenericArrayData(metadataJson.<List<String>>read(column.jpath).stream().map(StringData::fromString).toArray());
            } else {
                return new GenericArrayData(metadataJson.<List<Object>>read(column.jpath).toArray());
            }
        } else {
            throw new RuntimeException("Unsupported column type: " + column.type);
        }
    }

    @Override
    public TypeInformation<RowData> getProducedType() {
        return TypeInformation.of(RowData.class);
    }
}
