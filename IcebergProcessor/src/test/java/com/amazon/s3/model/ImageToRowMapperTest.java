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
import com.amazon.s3.schema.TableSchemaGenerator;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.*;

class ImageToRowMapperTest {

    private final EventDeserializationSchema deserializationSchema = new EventDeserializationSchema();

    @Test
    public void testThatChangeEventIsConvertedToRow() throws Exception {
        ImageRecord image = TestingHelpers.loadSampleEventAsImageRecord("CreateEvent");
        Map<String, Properties> defaultProperties = TestingHelpers.defaultCorrectProperties();
        ImageToRowDataMapper mapper = new ImageToRowDataMapper(TableSchemaGenerator.generate(defaultProperties, List.of()), List.of());
        RowData row = mapper.map(image);
        assertEquals("foo", row.getString(0).toString());
        assertEquals(RowKind.INSERT, row.getRowKind());
    }

    @Test
    public void testThatMapperRespectsCustomSchemaElements() throws Exception {
        ImageRecord imageRecord = TestingHelpers.loadSampleEventAsImageRecord("CreateEvent_AllMetadataTypes");
        Map<String, Properties> defaultProperties = TestingHelpers.defaultCorrectProperties();
        defaultProperties.get("schema").setProperty("custom_metadata_fields", "string_array, boolean_array, integer_array, string, integer, boolean");
        defaultProperties.get("schema").setProperty("field.string_array.jpath", "$.metadata.array.string");
        defaultProperties.get("schema").setProperty("field.string_array.type", "ARRAY<STRING>");
        defaultProperties.get("schema").setProperty("field.boolean_array.jpath", "$.metadata.array.boolean");
        defaultProperties.get("schema").setProperty("field.boolean_array.type", "ARRAY<BOOLEAN>");
        defaultProperties.get("schema").setProperty("field.integer_array.jpath", "$.metadata.array.integer");
        defaultProperties.get("schema").setProperty("field.integer_array.type", "ARRAY<INTEGER>");
        defaultProperties.get("schema").setProperty("field.string.jpath", "$.metadata.scalar.string");
        defaultProperties.get("schema").setProperty("field.string.type", "STRING");
        defaultProperties.get("schema").setProperty("field.integer.jpath", "$.metadata.scalar.integer");
        defaultProperties.get("schema").setProperty("field.integer.type", "INTEGER");
        defaultProperties.get("schema").setProperty("field.boolean.jpath", "$.metadata.scalar.boolean");
        defaultProperties.get("schema").setProperty("field.boolean.type", "BOOLEAN");

        List<TableSchemaGenerator.SchemaEntry> schemaEntries = TableSchemaGenerator.parseCustomMetadataFields(defaultProperties);
        ImageToRowDataMapper mapper = new ImageToRowDataMapper(TableSchemaGenerator.generate(defaultProperties, schemaEntries), schemaEntries);
        RowData row = mapper.map(imageRecord);

        assertEquals(RowKind.INSERT, row.getRowKind());
        assertArrayEquals(
                List.of(
                        StringData.fromString("foo"),
                        StringData.fromString("bar"),
                        StringData.fromString("baz")
                ).toArray(),
                ((GenericArrayData)row.getArray(7)).toObjectArray());
        assertArrayEquals(List.of(true, true, false).toArray(), ((GenericArrayData)row.getArray(8)).toObjectArray());
        assertArrayEquals(List.of(12, 13, 14).toArray(), ((GenericArrayData)row.getArray(9)).toObjectArray());
        assertEquals("foobar", row.getString(10).toString());
        assertEquals(12, row.getInt(11));
        assertTrue(row.getBoolean(12));
    }

    @Test
    public void testThatMapperGeneratesRowsForDeletes() throws Exception {
        Map<String, Properties> defaultProperties = TestingHelpers.defaultCorrectProperties();

        List<TableSchemaGenerator.SchemaEntry> schemaEntries = TableSchemaGenerator.parseCustomMetadataFields(defaultProperties);
        ImageToRowDataMapper mapper = new ImageToRowDataMapper(TableSchemaGenerator.generate(defaultProperties, schemaEntries), schemaEntries);
        RowData row = mapper.map(TestingHelpers.loadSampleEventAsImageRecord("DeleteEvent"));

        assertEquals(RowKind.DELETE, row.getRowKind());
    }

    @Test
    public void testThatMapperHandlesDeleteRowsWhenCustomFieldsAreDefined() throws Exception {
        Map<String, Properties> defaultProperties = TestingHelpers.defaultCorrectProperties();
        defaultProperties.get("schema").setProperty("custom_metadata_fields", "string_array");
        defaultProperties.get("schema").setProperty("field.string_array.jpath", "$.metadata.array.string");
        defaultProperties.get("schema").setProperty("field.string_array.type", "ARRAY<STRING>");

        List<TableSchemaGenerator.SchemaEntry> schemaEntries = TableSchemaGenerator.parseCustomMetadataFields(defaultProperties);
        ImageToRowDataMapper mapper = new ImageToRowDataMapper(TableSchemaGenerator.generate(defaultProperties, schemaEntries), schemaEntries);
        RowData row = mapper.map(TestingHelpers.loadSampleEventAsImageRecord("DeleteEvent"));

        assertEquals(RowKind.DELETE, row.getRowKind());
    }
}
