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

import com.amazon.s3.TestingHelpers;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static com.amazon.s3.configuration.ConfigurationProperties.*;
import static org.junit.jupiter.api.Assertions.*;

class ConfigurationPropertiesTest {

    @Test
    public void testPropertyIsExtracted() throws Exception {
        Map<String, Properties> propertiesMap = TestingHelpers.defaultCorrectProperties();
        propertiesMap.get("catalog").setProperty("name", "foo");

        assertEquals("foo", CATALOG_NAME.get(propertiesMap));
    }

    @Test
    public void testThrowsWhenRequiredPropertyIsMissing() throws Exception {
        Map<String, Properties> propertiesMap = new HashMap<>();
        Throwable t = assertThrows(MissingConfigurationParameter.class, () -> AWS_REGION.get(propertiesMap));
        assertTrue(t.getMessage().contains("sdk.region"));

        propertiesMap.put("sdk", new Properties());
        assertThrows(MissingConfigurationParameter.class, () -> AWS_REGION.get(propertiesMap));

        propertiesMap.get("sdk").setProperty("region", "foo");
        assertEquals("foo", AWS_REGION.get(propertiesMap));
    }

    @Test
    public void testThatDefaultReturnedWhenPropertyIsMissing() throws Exception {
        Map<String, Properties> propertiesMap = new HashMap<>();
        assertEquals(IO_IMPL.defaultValue, IO_IMPL.get(propertiesMap));

        propertiesMap.put("catalog", new Properties());
        assertEquals(IO_IMPL.defaultValue, IO_IMPL.get(propertiesMap));
    }
}