package org.lboutros.traveloptimizer.flink.jobs;


import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.formats.json.JsonDeserializationSchema;
import org.apache.flink.formats.json.JsonSerializationSchema;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.lboutros.traveloptimizer.flink.datagen.DataGeneratorJob;
import org.lboutros.traveloptimizer.flink.jobs.internalmodels.UnionEnvelope;
import org.lboutros.traveloptimizer.flink.processfunctions.OptimizerFunction;
import org.lboutros.traveloptimizer.model.CustomerTravelRequest;
import org.lboutros.traveloptimizer.model.PlaneTimeTableUpdate;
import org.lboutros.traveloptimizer.model.TrainTimeTableUpdate;
import org.lboutros.traveloptimizer.model.TravelAlert;

import java.io.InputStream;
import java.util.Properties;

public class TravelOptimizerJob {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties consumerConfig = new Properties();
        try (InputStream stream = TravelOptimizerJob.class.getClassLoader().getResourceAsStream("consumer.properties")) {
            consumerConfig.load(stream);
        }
        Properties producerConfig = new Properties();
        try (InputStream stream = TravelOptimizerJob.class.getClassLoader().getResourceAsStream("producer.properties")) {
            producerConfig.load(stream);
        }

        KafkaSource<PlaneTimeTableUpdate> planeKafkaSource = KafkaSource.<PlaneTimeTableUpdate>builder()
                .setProperties(consumerConfig)
                .setTopics("planeTimeUpdated")
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new JsonDeserializationSchema<>(PlaneTimeTableUpdate.class, DataGeneratorJob::getMapper))
                .build();

        KafkaSource<TrainTimeTableUpdate> trainKafkaSource = KafkaSource.<TrainTimeTableUpdate>builder()
                .setProperties(consumerConfig)
                .setTopics("trainTimeUpdated")
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new JsonDeserializationSchema<>(TrainTimeTableUpdate.class, DataGeneratorJob::getMapper))
                .build();

        KafkaSource<CustomerTravelRequest> requestKafkaSource = KafkaSource.<CustomerTravelRequest>builder()
                .setProperties(consumerConfig)
                .setTopics("customerTravelRequested")
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new JsonDeserializationSchema<>(CustomerTravelRequest.class, DataGeneratorJob::getMapper))
                .build();

        DataStreamSource<PlaneTimeTableUpdate> planeStreamSource =
                environment.fromSource(planeKafkaSource, WatermarkStrategy.noWatermarks(), "plane_source");

        DataStreamSource<TrainTimeTableUpdate> trainStreamSource =
                environment.fromSource(trainKafkaSource, WatermarkStrategy.noWatermarks(), "train_source");

        DataStreamSource<CustomerTravelRequest> requestStreamSource =
                environment.fromSource(requestKafkaSource, WatermarkStrategy.noWatermarks(), "request_source");


        // SINKS
        JsonSerializationSchema<TravelAlert> serializer = new JsonSerializationSchema<>(() ->
                new ObjectMapper()
                        .registerModule(new JavaTimeModule())
        );
        var kafkaSerializer = KafkaRecordSerializationSchema.<TravelAlert>builder()
                .setTopic("travelAlerts")
                .setValueSerializationSchema(serializer)
                .build();

        KafkaSink<TravelAlert> kafkaTravelAlertTopic = KafkaSink.<TravelAlert>builder()
                .setKafkaProducerConfig(producerConfig)
                .setRecordSerializer(kafkaSerializer)
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build();


        defineWorkflow(planeStreamSource, trainStreamSource, requestStreamSource).sinkTo(kafkaTravelAlertTopic);

        environment.execute("TravelOptimizer");
    }

    public static DataStream<TravelAlert> defineWorkflow(DataStream<PlaneTimeTableUpdate> planeStreamSource,
                                                         DataStream<TrainTimeTableUpdate> trainStreamSource,
                                                         DataStream<CustomerTravelRequest> requestStreamSource) {

        DataStream<UnionEnvelope> planeByLinkStream = planeStreamSource.map(UnionEnvelope::fromPlaneTimeTableUpdate);
        DataStream<UnionEnvelope> trainByLinkStream = trainStreamSource.map(UnionEnvelope::fromTrainTimeTableUpdate);
        DataStream<UnionEnvelope> requestByLinkStream = requestStreamSource.map(UnionEnvelope::fromCustomerTravelRequest);

        KeyedStream<UnionEnvelope, String> unionStream = planeByLinkStream.union(trainByLinkStream.union(requestByLinkStream))
                .keyBy(UnionEnvelope::getPartitionKey);

        return unionStream.process(new OptimizerFunction());
    }


}
