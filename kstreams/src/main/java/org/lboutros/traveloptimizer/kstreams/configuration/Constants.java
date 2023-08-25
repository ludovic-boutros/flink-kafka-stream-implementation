package org.lboutros.traveloptimizer.kstreams.configuration;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.kafka.common.serialization.Serde;
import org.lboutros.traveloptimizer.kstreams.topologies.models.TimeTableEntry;
import org.lboutros.traveloptimizer.kstreams.topologies.serdes.JsonSerde;
import org.lboutros.traveloptimizer.model.*;

public interface Constants {

    ObjectMapper OBJECT_MAPPER = new ObjectMapper()
            .registerModule(new JavaTimeModule());

    interface StateStores {
        String AVAILABLE_CONNECTIONS_STATE_STORE = "availableConnectionStateStore";
        String LAST_REQUEST_ALERT_STATE_STORE = "lastRequestAlertStateStore";
        String EARLY_REQUESTS_STATE_STORE = "earlyRequestStateStore";
    }

    interface InternalTopics {
        String REKEYED_CUSTOMER_TRAVEL_REQUEST_TOPIC = "rekeyedCustomerTravelRequested";
        String REKEYED_PLANE_TIME_UPDATE_TOPIC = "rekeyedPlaneTimeUpdated";
        String REKEYED_TRAIN_TIME_UPDATE_TOPIC = "rekeyedTrainTimeUpdated";
        String REKEYED_DEPARTURE_TOPIC = "rekeyedDeparted";
    }

    interface Serdes {
        Serde<CustomerTravelRequest> CUSTOMER_TRAVEL_REQUEST_SERDE = new JsonSerde<>() {
            @Override
            protected TypeReference<CustomerTravelRequest> getTypeReference() {
                return new TypeReference<>() {
                };
            }
        };

        Serde<Departure> DEPARTURE_SERDE = new JsonSerde<>() {
            @Override
            protected TypeReference<Departure> getTypeReference() {
                return new TypeReference<>() {
                };
            }
        };

        Serde<PlaneTimeTableUpdate> PLANE_TIME_UPDATE_SERDE = new JsonSerde<>() {
            @Override
            protected TypeReference<PlaneTimeTableUpdate> getTypeReference() {
                return new TypeReference<>() {
                };
            }
        };

        Serde<TrainTimeTableUpdate> TRAIN_TIME_UPDATE_SERDE = new JsonSerde<>() {
            @Override
            protected TypeReference<TrainTimeTableUpdate> getTypeReference() {
                return new TypeReference<>() {
                };
            }
        };

        Serde<TravelAlert> TRAVEL_ALERTS_SERDE = new JsonSerde<>() {
            @Override
            protected TypeReference<TravelAlert> getTypeReference() {
                return new TypeReference<>() {
                };
            }
        };

        Serde<TimeTableEntry> TIME_UPDATE_SERDE = new JsonSerde<>() {
            @Override
            protected TypeReference<TimeTableEntry> getTypeReference() {
                return new TypeReference<>() {
                };
            }
        };
    }
}
