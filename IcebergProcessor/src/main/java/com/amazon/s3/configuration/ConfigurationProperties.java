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

package com.amazon.s3.configuration;

import java.util.Map;
import java.util.Properties;

/**
 * Container for the various configuration properties the code expects.
 */
public enum ConfigurationProperties {

    AWS_REGION("sdk", "region"),
    AWS_ENDPOINT("sdk", "endpoint", null, false),
    STREAM_ARN("stream", "arn"),

    CATALOG_NAME("catalog", "name", "S3"),
    DATABASE_NAME("catalog", "database", "default"),
    TABLE_NAME("catalog", "table", "s3_content_metadata"),
    CATALOG_IMPL("catalog", "impl"),
    IO_IMPL("catalog", "io_impl", "org.apache.iceberg.aws.s3.S3FileIO"),
    WAREHOUSE_PATH("catalog", "warehousePath"),

    INCLUDE_RAW_METADATA("schema", "include_raw_metadata", "true"),
    CUSTOM_METADATA_FIELDS("schema", "custom_metadata_fields", null, false),
    FIELD_JPATH("schema", "field.%s.jpath"),
    FIELD_TYPE("schema", "field.%s.type"),
    ;

    private final String namespace;
    private final String propertyName;
    public final String defaultValue;
    private final boolean required;

    ConfigurationProperties(String namespace, String propertyName, String defaultValue, boolean required) {
        this.namespace = namespace;
        this.propertyName = propertyName;
        this.defaultValue = defaultValue;
        this.required = required;
    }

    ConfigurationProperties(String namespace, String propertyName, String defaultValue) {
        this(namespace, propertyName, defaultValue, false);
    }

    ConfigurationProperties(String namespace, String propertyName) {
        this(namespace, propertyName, null, true);
    }

    /**
     * Retrieves the configuration parameter from the provide properties map.
     *
     * @param properties
     * @throws MissingConfigurationParameter if the property was declared required
     * @return the configured property, null if not provided, or the default value if one was declared.
     */
    public String get(Map<String, Properties> properties) {
        return getPropertyValue(properties, propertyName);
    }

    /**
     * Retrieves the configuration parameter from the provided properties map, after parameterizing the
     * properties 'name'.
     * <p>
     * This functionality allows a property to be defined in terms of another property's value. For instance,
     * by defining a property containing names, and then for each of those names, defining subproperties.
     *
     * @see #CUSTOM_METADATA_FIELDS
     * @see #FIELD_TYPE
     * @see #FIELD_JPATH
     * @param properties
     * @param parameter The parameter with which to extend the property name.
     * @throws MissingConfigurationParameter if the property was declared required
     * @return the configured property, null if not provided, or the default value if one was declared.
     */
    public String getParameterized(Map<String, Properties> properties, String parameter) {
        String parameterizedPropertyName = String.format(propertyName, parameter);
        return getPropertyValue(properties, parameterizedPropertyName);
    }

    private String getPropertyValue(Map<String, Properties> properties, String parameterizedPropertyName) {
        String result = defaultValue;
        Properties props = properties.get(namespace);
        if (props != null) {
            String tmp = props.getProperty(parameterizedPropertyName);
            if (tmp != null) {
                result = tmp;
            }
        }

        if (this.required && result == null) {
            throw new MissingConfigurationParameter(namespace, parameterizedPropertyName);
        }
        return result;
    }
}
