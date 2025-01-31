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
package com.amazon.s3.schema;

import com.amazon.s3.TestingHelpers;
import com.amazon.s3.configuration.MissingConfigurationParameter;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.ValidationException;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.*;

class TableSchemaGeneratorTest {

    @Test
    public void testCustomSchemaFieldParsing() throws Exception {
        Map<String, Properties> properties = TestingHelpers.defaultCorrectProperties();
        Properties props = new Properties();
        props.setProperty("custom_metadata_fields", "foo");
        props.setProperty("field.foo.type", "STRING");
        props.setProperty("field.foo.jpath", "$.labels[0].label");
        properties.put("schema", props);

        List<TableSchemaGenerator.SchemaEntry> schemaEntries = TableSchemaGenerator.parseCustomMetadataFields(properties);
        assertNotNull(schemaEntries);
        assertEquals(1, schemaEntries.size());
        TableSchemaGenerator.SchemaEntry schemaEntry = schemaEntries.get(0);
        assertEquals("foo", schemaEntry.name);
        assertEquals(DataTypes.STRING(), schemaEntry.type);
        assertEquals("$.labels[0].label", schemaEntry.jpath);
    }

    @Test
    public void testThatParsingThrowsWhenMissingProperties() throws Exception {
        Properties props = new Properties();
        props.setProperty("custom_metadata_fields", "foo, bar");
        props.setProperty("field.foo.type", "STRING");
        props.setProperty("field.foo.jpath", "$.labels[0].label");
        Map<String, Properties> properties = Map.of("schema", props);

        assertThrows(MissingConfigurationParameter.class, () -> TableSchemaGenerator.parseCustomMetadataFields(properties));
    }

    @Test
    public void testThatParsingThrowsWhenInvalidProperties() throws Exception {
        Properties props = new Properties();
        props.setProperty("custom_metadata_fields", "foo");
        props.setProperty("field.foo.type", "NotAValidType");
        props.setProperty("field.foo.jpath", "$.labels[0].label");
        Map<String, Properties> properties = Map.of("schema", props);

        assertThrows(ValidationException.class, () -> TableSchemaGenerator.parseCustomMetadataFields(properties));
    }

    @Test
    public void testThatParsingThrowsWhenUnsupportedType() throws Exception {
        Properties props = new Properties();
        props.setProperty("custom_metadata_fields", "foo");
        props.setProperty("field.foo.type", "MAP<STRING, STRING>");
        props.setProperty("field.foo.jpath", "$.labels[0].label");
        Map<String, Properties> properties = Map.of("schema", props);

        assertThrows(ValidationException.class, () -> TableSchemaGenerator.parseCustomMetadataFields(properties));
    }

    @Test
    public void testThatParsingHandlesMultipleFieldDefinitions() throws Exception {
        Properties props = new Properties();
        props.setProperty("custom_metadata_fields", "foo, bar");
        props.setProperty("field.foo.type", "STRING");
        props.setProperty("field.foo.jpath", "$.labels[0].label");
        props.setProperty("field.bar.type", "STRING");
        props.setProperty("field.bar.jpath", "$.something[0].else");
        Map<String, Properties> properties = Map.of("schema", props);

        List<TableSchemaGenerator.SchemaEntry> schemaEntries = TableSchemaGenerator.parseCustomMetadataFields(properties);

        assertNotNull(schemaEntries);
        assertEquals(2, schemaEntries.size());
        TableSchemaGenerator.SchemaEntry fooEntry = schemaEntries.get(0);
        TableSchemaGenerator.SchemaEntry barEntry = schemaEntries.get(1);

        assertEquals("$.labels[0].label", fooEntry.jpath);
        assertEquals("$.something[0].else", barEntry.jpath);
    }

    @Test
    public void testFullSQLGenerationPath() throws Exception {
        Map<String, Properties> props = TestingHelpers.defaultCorrectProperties();
        props.get("schema").setProperty("custom_metadata_fields", "foo");
        props.get("schema").setProperty("field.foo.type", "STRING");
        props.get("schema").setProperty("field.foo.jpath", "$.labels[0].label");

        List<TableSchemaGenerator.SchemaEntry> schemaEntries = TableSchemaGenerator.parseCustomMetadataFields(props);
        Schema tableSchema = TableSchemaGenerator.generate(props, schemaEntries);

        String sql = TableSchemaGenerator.generateTableSQL(props, tableSchema);
        assertTrue(sql.contains("`foo` STRING"));
    }
}