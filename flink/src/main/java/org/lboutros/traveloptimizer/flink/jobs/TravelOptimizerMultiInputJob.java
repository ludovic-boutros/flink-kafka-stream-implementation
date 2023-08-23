package org.lboutros.traveloptimizer.flink.jobs;


import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
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
import org.apache.flink.streaming.api.datastream.MultipleConnectedStreams;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.transformations.KeyedMultipleInputTransformation;
import org.lboutros.traveloptimizer.GlobalConstants;
import org.lboutros.traveloptimizer.flink.datagen.DataGeneratorJob;
import org.lboutros.traveloptimizer.flink.jobs.internalmodels.Utils;
import org.lboutros.traveloptimizer.flink.jobs.operators.TravelOptimizerOperator;
import org.lboutros.traveloptimizer.flink.serializers.TravelAlertKeyStringSerializer;
import org.lboutros.traveloptimizer.model.*;

import java.io.InputStream;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Properties;

public class TravelOptimizerMultiInputJob {
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
                .setKafkaKeySerializer(TravelAlertKeyStringSerializer.class)
                .setValueSerializationSchema(serializer)
                .build();

        KafkaSink<TravelAlert> kafkaTravelAlertTopic = KafkaSink.<TravelAlert>builder()
                .setKafkaProducerConfig(producerConfig)
                .setRecordSerializer(kafkaSerializer)
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build();

        defineWorkflow(planeStreamSource, trainStreamSource, requestStreamSource, departureStreamSource, environment).sinkTo(kafkaTravelAlertTopic);

        environment.execute("TravelOptimizer");
    }

    public static DataStream<TravelAlert> defineWorkflow(DataStream<PlaneTimeTableUpdate> planeStreamSource,
                                                         DataStream<TrainTimeTableUpdate> trainStreamSource,
                                                         DataStream<CustomerTravelRequest> requestStreamSource,
                                                         DataStream<Departure> departureStreamSource,
                                                         StreamExecutionEnvironment env) {

        var planeByLinkStream = planeStreamSource
                // Filter updates too close from the current time in order to mitigate the time table update zombies
                // caused by the race condition between departure events and time table update events.
                .filter(p -> p.getDepartureTime().isAfter(ZonedDateTime.now(ZoneId.of("UTC")).plusMinutes(5)));

        SingleOutputStreamOperator<TrainTimeTableUpdate> trainByLinkStream = trainStreamSource
                // Filter updates too close from the current time in order to mitigate the time table update zombies
                // caused by the race condition between departure events and time table update events.
                .filter(t -> t.getDepartureTime().isAfter(ZonedDateTime.now(ZoneId.of("UTC")).plusMinutes(5)));

        KeyedMultipleInputTransformation<TravelAlert> transform =
                new KeyedMultipleInputTransformation<>(
                        "Multi Type Input Operator",
                        new TravelOptimizerOperator.TravelOptimizerOperatorFactory(),
                        TypeInformation.of(TravelAlert.class), // Output Type
                        1, // ?
                        BasicTypeInfo.STRING_TYPE_INFO); // Partition key type ?


        KeySelector<PlaneTimeTableUpdate, String> planeEventKeySelector = value -> Utils.getPartitionKey(value.getDepartureLocation(), value.getArrivalLocation());
        KeySelector<TrainTimeTableUpdate, String> trainEventKeySelector = value -> Utils.getPartitionKey(value.getDepartureLocation(), value.getArrivalLocation());
        KeySelector<CustomerTravelRequest, String> requestEventKeySelector = value -> Utils.getPartitionKey(value.getDepartureLocation(), value.getArrivalLocation());
        KeySelector<Departure, String> departureEventKeySelector = value -> Utils.getPartitionKey(value.getDepartureLocation(), value.getArrivalLocation());

        env.addOperator(
                transform
                        .addInput(planeByLinkStream.getTransformation(), planeEventKeySelector)
                        .addInput(trainByLinkStream.getTransformation(), trainEventKeySelector)
                        .addInput(requestStreamSource.getTransformation(), requestEventKeySelector)
                        .addInput(departureStreamSource.getTransformation(), departureEventKeySelector));


        return new MultipleConnectedStreams(env).transform(transform);
    }


}
