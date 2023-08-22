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
import org.lboutros.traveloptimizer.GlobalConstants;
import org.lboutros.traveloptimizer.flink.datagen.DataGeneratorJob;
import org.lboutros.traveloptimizer.flink.jobs.internalmodels.UnionEnvelope;
import org.lboutros.traveloptimizer.flink.jobs.processfunctions.OptimizerFunction;
import org.lboutros.traveloptimizer.model.*;

import java.io.InputStream;
import java.time.ZoneId;
import java.time.ZonedDateTime;
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
                .setTopics(GlobalConstants.Topics.PLANE_TIME_UPDATE_TOPIC)
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new JsonDeserializationSchema<>(PlaneTimeTableUpdate.class, DataGeneratorJob::getMapper))
                .build();

        KafkaSource<TrainTimeTableUpdate> trainKafkaSource = KafkaSource.<TrainTimeTableUpdate>builder()
                .setProperties(consumerConfig)
                .setTopics(GlobalConstants.Topics.TRAIN_TIME_UPDATE_TOPIC)
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new JsonDeserializationSchema<>(TrainTimeTableUpdate.class, DataGeneratorJob::getMapper))
                .build();

        KafkaSource<CustomerTravelRequest> requestKafkaSource = KafkaSource.<CustomerTravelRequest>builder()
                .setProperties(consumerConfig)
                .setTopics(GlobalConstants.Topics.CUSTOMER_TRAVEL_REQUEST_TOPIC)
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new JsonDeserializationSchema<>(CustomerTravelRequest.class, DataGeneratorJob::getMapper))
                .build();

        KafkaSource<Departure> departureKafkaSource = KafkaSource.<Departure>builder()
                .setProperties(consumerConfig)
                .setTopics(GlobalConstants.Topics.DEPARTURE_TOPIC)
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new JsonDeserializationSchema<>(Departure.class, DataGeneratorJob::getMapper))
                .build();

        DataStreamSource<PlaneTimeTableUpdate> planeStreamSource =
                environment.fromSource(planeKafkaSource, WatermarkStrategy.noWatermarks(), "plane_source");

        DataStreamSource<TrainTimeTableUpdate> trainStreamSource =
                environment.fromSource(trainKafkaSource, WatermarkStrategy.noWatermarks(), "train_source");

        DataStreamSource<CustomerTravelRequest> requestStreamSource =
                environment.fromSource(requestKafkaSource, WatermarkStrategy.noWatermarks(), "request_source");

        DataStreamSource<Departure> departureStreamSource =
                environment.fromSource(departureKafkaSource, WatermarkStrategy.noWatermarks(), "departure_source");

        // SINKS
        JsonSerializationSchema<TravelAlert> serializer = new JsonSerializationSchema<>(() ->
                new ObjectMapper()
                        .registerModule(new JavaTimeModule())
        );
        var kafkaSerializer = KafkaRecordSerializationSchema.<TravelAlert>builder()
                .setTopic(GlobalConstants.Topics.TRAVEL_ALERTS_TOPIC)
                .setValueSerializationSchema(serializer)
                .build();

        KafkaSink<TravelAlert> kafkaTravelAlertTopic = KafkaSink.<TravelAlert>builder()
                .setKafkaProducerConfig(producerConfig)
                .setRecordSerializer(kafkaSerializer)
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build();

        defineWorkflow(planeStreamSource, trainStreamSource, requestStreamSource, departureStreamSource).sinkTo(kafkaTravelAlertTopic);

        environment.execute("TravelOptimizer");
    }

    public static DataStream<TravelAlert> defineWorkflow(DataStream<PlaneTimeTableUpdate> planeStreamSource,
                                                         DataStream<TrainTimeTableUpdate> trainStreamSource,
                                                         DataStream<CustomerTravelRequest> requestStreamSource,
                                                         DataStream<Departure> departureStreamSource) {

        DataStream<UnionEnvelope> planeByLinkStream = planeStreamSource
                // Filter updates too close from the current time in order to mitigate the time table update zombies
                // caused by the race condition between departure events and time table update events.
                .filter(p -> p.getDepartureTime().isAfter(ZonedDateTime.now(ZoneId.of("UTC")).plusMinutes(5)))
                .map(UnionEnvelope::fromPlaneTimeTableUpdate);

        DataStream<UnionEnvelope> trainByLinkStream = trainStreamSource
                // Filter updates too close from the current time in order to mitigate the time table update zombies
                // caused by the race condition between departure events and time table update events.
                .filter(t -> t.getDepartureTime().isAfter(ZonedDateTime.now(ZoneId.of("UTC")).plusMinutes(5)))
                .map(UnionEnvelope::fromTrainTimeTableUpdate);

        DataStream<UnionEnvelope> requestByLinkStream = requestStreamSource.map(UnionEnvelope::fromCustomerTravelRequest);
        DataStream<UnionEnvelope> departureByLinkStream = departureStreamSource.map(UnionEnvelope::fromDeparture);

        KeyedStream<UnionEnvelope, String> unionStream = planeByLinkStream
                .union(trainByLinkStream)
                .union(requestByLinkStream)
                .union(departureByLinkStream)
                .keyBy(UnionEnvelope::getPartitionKey);

        return unionStream.process(new OptimizerFunction());
    }


}
