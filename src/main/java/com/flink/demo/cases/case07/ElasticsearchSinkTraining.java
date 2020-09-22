package com.flink.demo.cases.case07;

import com.flink.demo.cases.common.datasource.AllDataTypeDataSource;
import com.flink.demo.cases.common.datasource.UrlClickRowDataSource;
import com.flink.demo.cases.common.utils.ConvertUtil;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch.util.RetryRejectedExecutionFailureHandler;
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink;
import org.apache.flink.types.Row;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;
import org.elasticsearch.client.RestClientBuilder;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.*;

public class ElasticsearchSinkTraining {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        RowTypeInfo rowTypeInfo = (RowTypeInfo) AllDataTypeDataSource.allDataTypeInfo;

        SingleOutputStreamOperator source = env.addSource(new AllDataTypeDataSource())
                .returns(rowTypeInfo)
                .name("url click source");

        List<HttpHost> httpHosts = new ArrayList<>();
        httpHosts.add(new HttpHost("192.168.1.82", 9204, "http"));

        // use a ElasticsearchSink.Builder to create an ElasticsearchSink
        ElasticsearchSink.Builder<Row> esSinkBuilder = new ElasticsearchSink.Builder<>(
                httpHosts,
                new ElasticsearchSinkFunction<Row>() {
                    public IndexRequest createIndexRequest(Row row) {
                        Map<String, Object> json = new HashMap<>();
                        String[] fieldNames = rowTypeInfo.getFieldNames();
                        for (String fieldName : fieldNames) {
                            int fieldIndex = rowTypeInfo.getFieldIndex(fieldName);
                            Object fieldValue = row.getField(fieldIndex);
                            if (fieldValue != null) {
                                String type = rowTypeInfo.getTypeAt(fieldName).toString();
                                Object convertedV = convert(fieldValue, type);
                                json.put(fieldName, convertedV);
                            } else {
                                json.put(fieldName, null);
                            }
                        }

                        return Requests.indexRequest()
                                .index("shiy-flink-sink-test")
                                .type("")
                                .source(json);
                    }

                    @Override
                    public void process(Row element, RuntimeContext ctx, RequestIndexer indexer) {
                        indexer.add(createIndexRequest(element));
                    }
                }
        );

        // configuration for the bulk requests; this instructs the sink to emit after every element, otherwise they would be buffered
        esSinkBuilder.setBulkFlushMaxActions(1);

        esSinkBuilder.setFailureHandler(new RetryRejectedExecutionFailureHandler());

        /*// provide a RestClientFactory for custom configuration on the internally created REST client
        esSinkBuilder.setRestClientFactory(
                restClientBuilder -> {
                    restClientBuilder.setDefaultHeaders(...)
                    restClientBuilder.setMaxRetryTimeoutMillis(...)
                    restClientBuilder.setPathPrefix(...)
                    restClientBuilder.setHttpClientConfigCallback(...)
                }
        );*/

        // provide a RestClientFactory for custom configuration on the internally created REST client
        esSinkBuilder.setRestClientFactory(
                restClientBuilder -> {
                    restClientBuilder.setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
                        @Override
                        public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpClientBuilder) {

                            // elasticsearch username and password
                            CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
                            credentialsProvider.setCredentials(AuthScope.ANY,
                                    new UsernamePasswordCredentials("admin", "admin"));

                            return httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
                        }
                    });
                }
        );

        // finally, build and add the sink to the job's pipeline
        source.addSink(esSinkBuilder.build());

        env.execute();
    }

    private static Object convert(Object value, String type) {
        switch (type.toLowerCase()) {
            case "byte":
            case "short":
            case "int":
            case "integer":
            case "bigint":
            case "long":
            case "float":
            case "double":
            case "string":
            case "boolean":
                return value;
            case "byte[]":
            case "binary":
                byte[] bytes = (byte[]) value;
                return Base64.getEncoder().encodeToString(bytes);
            case "date":
                Date date = (Date) value;
                return date.getTime();
            case "timestamp":
                Timestamp timestamp = (Timestamp) value;
                return timestamp.getTime();
            case "decimal":
            case "bigdecimal":
                BigDecimal bigDecimal = (BigDecimal) value;
                return bigDecimal.doubleValue();
            default:
                throw new RuntimeException("Not support type " + type);
        }
    }

}
