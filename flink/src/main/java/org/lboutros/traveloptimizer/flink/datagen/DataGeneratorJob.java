package org.lboutros.traveloptimizer.flink.datagen;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.formats.json.JsonSerializationSchema;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.lboutros.traveloptimizer.GlobalConstants;
import org.lboutros.traveloptimizer.flink.serializer.CustomerTravelRequestKeyStringSerializer;
import org.lboutros.traveloptimizer.flink.serializer.DepartureKeyStringSerializer;
import org.lboutros.traveloptimizer.flink.serializer.PlaneTimeTableUpdateKeyStringSerializer;
import org.lboutros.traveloptimizer.flink.serializer.TrainTimeTableUpdateKeyStringSerializer;
import org.lboutros.traveloptimizer.model.CustomerTravelRequest;
import org.lboutros.traveloptimizer.model.Departure;
import org.lboutros.traveloptimizer.model.PlaneTimeTableUpdate;
import org.lboutros.traveloptimizer.model.TrainTimeTableUpdate;

import java.io.InputStream;
import java.util.Properties;

public class DataGeneratorJob {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties producerConfig = new Properties();
        try (InputStream stream = DataGeneratorJob.class.getClassLoader().getResourceAsStream("producer.properties")) {
            producerConfig.load(stream);
        }

        DataGeneratorSource<PlaneTimeTableUpdate> planeSource =
                new DataGeneratorSource<>(
                        index -> DataGenerator.generatePlaneData(),
                        Long.MAX_VALUE,
                        RateLimiterStrategy.perSecond(1),
                        Types.POJO(PlaneTimeTableUpdate.class)
                );

        DataGeneratorSource<TrainTimeTableUpdate> trainSource =
                new DataGeneratorSource<>(
                        index -> DataGenerator.generateTrainData(),
                        Long.MAX_VALUE,
                        RateLimiterStrategy.perSecond(1),
                        Types.POJO(TrainTimeTableUpdate.class)
                );

        DataGeneratorSource<CustomerTravelRequest> requestSource =
                new DataGeneratorSource<>(
                        index -> DataGenerator.generateCustomerTravelRequestData(),
                        Long.MAX_VALUE,
                        RateLimiterStrategy.perSecond(100),
                        Types.POJO(CustomerTravelRequest.class)
                );

        DataGeneratorSource<Departure> departureSource =
                new DataGeneratorSource<>(
                        index -> DataGenerator.generateDepartureData(),
                        Long.MAX_VALUE,
                        RateLimiterStrategy.perSecond(1),
                        Types.POJO(Departure.class)
                );

        DataStream<PlaneTimeTableUpdate> planeStream = env
                .fromSource(planeSource, WatermarkStrategy.noWatermarks(), "plane_source");

        DataStream<TrainTimeTableUpdate> trainStream = env
                .fromSource(trainSource, WatermarkStrategy.noWatermarks(), "train_source");

        DataStream<CustomerTravelRequest> requestStream = env
                .fromSource(requestSource, WatermarkStrategy.noWatermarks(), "request_source");

        DataStream<Departure> departureStream = env
                .fromSource(departureSource, WatermarkStrategy.noWatermarks(), "departure_source");

        KafkaRecordSerializationSchema<PlaneTimeTableUpdate> planeSerializer = KafkaRecordSerializationSchema.<PlaneTimeTableUpdate>builder()
                .setTopic(GlobalConstants.Topics.PLANE_TIME_UPDATE_TOPIC)
                .setKafkaKeySerializer(PlaneTimeTableUpdateKeyStringSerializer.class)
                .setValueSerializationSchema(new JsonSerializationSchema<>(DataGeneratorJob::getMapper))
                .build();

        KafkaRecordSerializationSchema<TrainTimeTableUpdate> trainSerializer = KafkaRecordSerializationSchema.<TrainTimeTableUpdate>builder()
                .setTopic(GlobalConstants.Topics.TRAIN_TIME_UPDATE_TOPIC)
                .setKafkaKeySerializer(TrainTimeTableUpdateKeyStringSerializer.class)
                .setValueSerializationSchema(new JsonSerializationSchema<>(DataGeneratorJob::getMapper))
                .build();

        KafkaRecordSerializationSchema<CustomerTravelRequest> requestSerializer = KafkaRecordSerializationSchema.<CustomerTravelRequest>builder()
                .setTopic(GlobalConstants.Topics.CUSTOMER_TRAVEL_REQUEST_TOPIC)
                .setKafkaKeySerializer(CustomerTravelRequestKeyStringSerializer.class)
                .setValueSerializationSchema(new JsonSerializationSchema<>(DataGeneratorJob::getMapper))
                .build();

        KafkaRecordSerializationSchema<Departure> departureSerializer = KafkaRecordSerializationSchema.<Departure>builder()
                .setTopic(GlobalConstants.Topics.DEPARTURE_TOPIC)
                .setKafkaKeySerializer(DepartureKeyStringSerializer.class)
                .setValueSerializationSchema(new JsonSerializationSchema<>(DataGeneratorJob::getMapper))
                .build();

        KafkaSink<PlaneTimeTableUpdate> planeSink = KafkaSink.<PlaneTimeTableUpdate>builder()
                .setKafkaProducerConfig(producerConfig)
                .setRecordSerializer(planeSerializer)
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build();

        KafkaSink<TrainTimeTableUpdate> trainSink = KafkaSink.<TrainTimeTableUpdate>builder()
                .setKafkaProducerConfig(producerConfig)
                .setRecordSerializer(trainSerializer)
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build();

        KafkaSink<CustomerTravelRequest> requestSink = KafkaSink.<CustomerTravelRequest>builder()
                .setKafkaProducerConfig(producerConfig)
                .setRecordSerializer(requestSerializer)
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build();

        KafkaSink<Departure> departureSink = KafkaSink.<Departure>builder()
                .setKafkaProducerConfig(producerConfig)
                .setRecordSerializer(departureSerializer)
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build();

        planeStream
                .sinkTo(planeSink)
                .name("plane_sink");

        trainStream
                .sinkTo(trainSink)
                .name("train_sink");

        requestStream
                .sinkTo(requestSink)
                .name("request_sink");

        departureStream
                .sinkTo(departureSink)
                .name("departure_sink");

        env.execute("InputStreams");
    }

    public static ObjectMapper getMapper() {
        return new ObjectMapper().registerModule(new JavaTimeModule());
    }
}
