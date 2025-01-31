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

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.*;
import org.apache.flink.table.types.logical.utils.LogicalTypeParser;
import org.apache.flink.table.types.utils.LogicalTypeDataTypeConverter;

import java.io.Serializable;
import java.util.*;
import java.util.stream.Collectors;

import static com.amazon.s3.configuration.ConfigurationProperties.*;

public class TableSchemaGenerator {

    public static Schema generate(Map<String, Properties> config, List<SchemaEntry> customMetadataFields) {
        Schema.Builder builder = Schema.newBuilder()
                .column("bucket", DataTypes.STRING().notNull())
                .column("key", DataTypes.STRING().notNull())
                .column("versionId", DataTypes.STRING())
                .column("sequencer", DataTypes.STRING().notNull())
                .column("etag", DataTypes.STRING());

        if (INCLUDE_RAW_METADATA.get(config).equalsIgnoreCase("true")) {
            builder.column("metadata", DataTypes.STRING());
        }
        builder.column("lastModified", DataTypes.TIMESTAMP());

        customMetadataFields.forEach(x -> builder.column(x.name, x.type));
        return builder.build();
    }

    /**
     * Parses the configuration of metadata field definitions from the raw json.
     * <p>
     * Fields names are defined by a comma-delimited list in the 'custom_metadata_fields' property entry.  Then, for
     * each custom metadata field defined in that list, two properties are expected: field.<name>.type and
     * field.<name>.jpath.
     *
     * @param config
     * @return a list of the elevated metadata fields
     */
    public static List<SchemaEntry> parseCustomMetadataFields(Map<String, Properties> config) {
        String namesString = CUSTOM_METADATA_FIELDS.get(config);
        if (namesString == null || namesString.isEmpty()) {
            return Collections.emptyList();
        }

        return Arrays.stream(namesString.split(","))
                .map(String::trim)
                .map((x) -> {
                    String jpath = FIELD_JPATH.getParameterized(config, x);
                    String dataTypeString = FIELD_TYPE.getParameterized(config, x);
                    DataType type = determineDataType(dataTypeString);
                    return new SchemaEntry(x, type, jpath);
                })
                .collect(Collectors.toList());
    }

    private static DataType determineDataType(String datatypeName) throws RuntimeException {
        LogicalType logicalType = LogicalTypeParser.parse(datatypeName, TableSchemaGenerator.class.getClassLoader());
        logicalType.accept(new TypeValidationVisitor());
        return LogicalTypeDataTypeConverter.toDataType(logicalType);
    }

    public static String generateTableSQL(Map<String, Properties> config, Schema schema) {
        StringBuilder builder = new StringBuilder();
        builder.append(String.format(
                "CREATE TABLE IF NOT EXISTS `%s`.`%s`.`%s` (\n",
                CATALOG_NAME.get(config),
                DATABASE_NAME.get(config),
                TABLE_NAME.get(config)));

        schema.getColumns().forEach(x ->
                builder.append(String.format("  `%s` %s,\n", x.getName(), ((Schema.UnresolvedPhysicalColumn)x).getDataType())));

        builder.append("  PRIMARY KEY(`bucket`, `key`) NOT ENFORCED\n)\n")
                .append("WITH (\n")
                .append("  'format-version'='2',\n")
                .append("  'write.upsert.enabled'='true'\n")
                .append(");");

        return builder.toString();
    }

    public static class SchemaEntry implements Serializable {
        public final String name;
        public final DataType type;
        public final String jpath;

        public SchemaEntry(String name, DataType type, String jpath) {
            this.name = name;
            this.type = type;
            this.jpath = jpath;
        }
    }
}
