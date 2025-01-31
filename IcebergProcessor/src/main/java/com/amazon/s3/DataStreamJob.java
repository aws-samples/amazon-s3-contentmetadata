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

import com.amazon.s3.model.EventDeserializationSchema;
import com.amazon.s3.model.ImageRecord;
import com.amazon.s3.model.ImageToRowDataMapper;
import com.amazon.s3.schema.TableSchemaGenerator;
import com.amazonaws.services.kinesisanalytics.runtime.KinesisAnalyticsRuntime;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.dynamodb.source.DynamoDbStreamsSource;
import org.apache.flink.connector.dynamodb.source.DynamoDbStreamsSourceBuilder;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kinesis.config.AWSConfigConstants;
import org.apache.flink.streaming.connectors.kinesis.config.ConsumerConfigConstants;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.data.RowData;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.flink.CatalogLoader;
import org.apache.iceberg.flink.FlinkCatalog;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.sink.FlinkSink;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.net.URL;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static com.amazon.s3.configuration.ConfigurationProperties.*;

public class DataStreamJob {
    private static final Logger LOG = LogManager.getLogger(DataStreamJob.class);

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        Map<String, Properties> config = loadProperties(env, args);

        ExecutionConfig executionConfig = env.getConfig();
        executionConfig.setAutoWatermarkInterval(Duration.ofMinutes(1).toMillis());

        CatalogLoader catalogLoader = getCatalogLoader(config);
        FlinkCatalog catalog = configureCatalog(catalogLoader, config);
        List<TableSchemaGenerator.SchemaEntry> customMetadataEntries = TableSchemaGenerator.parseCustomMetadataFields(config);
        Schema tableSchema = TableSchemaGenerator.generate(config, customMetadataEntries);
        createTable(env, config, tableSchema, catalog);

        SingleOutputStreamOperator<RowData> ddbChangeStream = env.fromSource(
                        createEventSource(config),
                        WatermarkStrategy.forMonotonousTimestamps(),
                        "DDB Change Stream"
                )
                .returns(TypeInformation.of(ImageRecord.class))
                .map(new ImageToRowDataMapper(tableSchema, customMetadataEntries));

        TableLoader tableLoader = TableLoader.fromCatalog(
                catalogLoader,
                TableIdentifier.of(
                        DATABASE_NAME.get(config),
                        TABLE_NAME.get(config)));

        FlinkSink.forRowData(ddbChangeStream)
            .tableLoader(tableLoader)
                .upsert(true)
                .append();

        env.execute("Iceberg Processor");
    }

    private static DynamoDbStreamsSource<ImageRecord> createEventSource(Map<String, Properties> config) {
        org.apache.flink.configuration.Configuration dynamodbStreamsConsumerConfig =
                new org.apache.flink.configuration.Configuration();
        dynamodbStreamsConsumerConfig.setString(AWSConfigConstants.AWS_REGION, AWS_REGION.get(config));
        dynamodbStreamsConsumerConfig.setString(ConsumerConfigConstants.SHARD_IDLE_INTERVAL_MILLIS, "60_000");

        return new DynamoDbStreamsSourceBuilder<ImageRecord>()
                .setStreamArn(STREAM_ARN.get(config))
                .setDeserializationSchema(new EventDeserializationSchema())
                .setSourceConfig(dynamodbStreamsConsumerConfig)
                .build();
    }

    private static Map<String, Properties> loadProperties(StreamExecutionEnvironment env, String[] args) throws IOException {
        if(isLocal(env)) {
            env.enableCheckpointing(5000);
            env.setParallelism(2);

            String localConfig = "IcebergProcessor-Properties-TableBuckets.json";
            for (int i = 0; i < args.length; i++) {
                if (args[i].startsWith("--ConfigFile") && args.length >= i + 1) {
                    localConfig = args[i + 1];
                    break;
                }
            }

            LOG.info("Loading local application configuration from {}", localConfig);
            URL configurationFile = DataStreamJob.class.getClassLoader().getResource(localConfig);
            if (configurationFile == null) {
                throw new RuntimeException("Could not find configuration file: " + localConfig);
            }
            return KinesisAnalyticsRuntime.getApplicationProperties(configurationFile.getPath());
        } else {
            return KinesisAnalyticsRuntime.getApplicationProperties();
        }
    }


    private static FlinkCatalog configureCatalog(CatalogLoader loader, Map<String, Properties> config) {
        return new FlinkCatalog(
                CATALOG_NAME.get(config),
                DATABASE_NAME.get(config),
                Namespace.empty(),
                loader,
                true,
                -1);
    }

    private static CatalogLoader getCatalogLoader(Map<String, Properties> config) {
        Configuration hadoopConf = new Configuration(false);
        Map<String, String> catalogProperties = new HashMap<>();
        catalogProperties.put("warehouse", WAREHOUSE_PATH.get(config));
        catalogProperties.put("catalog-impl", CATALOG_NAME.get(config));
        catalogProperties.put("io-impl", IO_IMPL.get(config));
        String endpointOverride = AWS_ENDPOINT.get(config);
        if (endpointOverride != null) {
            catalogProperties.put("s3tables.endpoint", endpointOverride);
        }
        catalogProperties.put("s3.delete-enabled", "false");


        return CatalogLoader.custom(
                CATALOG_NAME.get(config),
                catalogProperties,
                hadoopConf,
                CATALOG_IMPL.get(config));
    }

    private static void createTable(StreamExecutionEnvironment env, Map<String, Properties> config, Schema schema, FlinkCatalog catalog) {
        final StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        final String catalogName = CATALOG_NAME.get(config);
        final String tableCreationSql = TableSchemaGenerator.generateTableSQL(config, schema);

        tableEnv.registerCatalog(catalogName, catalog);
        tableEnv.useCatalog(catalogName);

        try {
            tableEnv.executeSql(String.format("CREATE DATABASE IF NOT EXISTS `%s`.`%s`", catalogName, DATABASE_NAME.get(config)));
        } catch (TableException tbl) {
            if (!tbl.getCause().getMessage().contains("A namespace with an identical name already exists")) {
                throw tbl;
            } else {
                LOG.info("Namespace already created, but catalog threw anyway.", tbl);
            }
        }
        tableEnv.executeSql(tableCreationSql);
    }

    private static boolean isLocal(StreamExecutionEnvironment env) {
        return env instanceof LocalStreamEnvironment;
    }

}
