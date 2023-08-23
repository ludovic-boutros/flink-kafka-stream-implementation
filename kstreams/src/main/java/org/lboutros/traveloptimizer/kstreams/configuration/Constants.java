package org.lboutros.traveloptimizer.kstreams.configuration;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Serde;
import org.lboutros.traveloptimizer.kstreams.models.TimeTableUpdate;
import org.lboutros.traveloptimizer.kstreams.serdes.JsonSerde;
import org.lboutros.traveloptimizer.model.CustomerTravelRequest;
import org.lboutros.traveloptimizer.model.PlaneTimeTableUpdate;
import org.lboutros.traveloptimizer.model.TrainTimeTableUpdate;
import org.lboutros.traveloptimizer.model.TravelAlert;

public interface Constants {

    ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    interface StateStores {
        String AVAILABLE_CONNECTIONS_STATE_STORE = "availableConnectionStateStore";
        String ACTIVE_REQUESTS_STATE_STORE = "activeRequestsStateStore";
        String EARLY_REQUESTS_STATE_STORE = "earlyRequestStateStore";
    }

    interface InternalTopics {
        String REKEYED_CUSTOMER_TRAVEL_REQUEST_TOPIC = "rekeyedCustomerTravelRequested";
        String REKEYED_PLANE_TIME_UPDATE_TOPIC = "rekeyedPlaneTimeUpdated";
        String REKEYED_TRAIN_TIME_UPDATE_TOPIC = "rekeyedTrainTimeUpdated";
    }

    interface Serdes {
        Serde<CustomerTravelRequest> CUSTOMER_TRAVEL_REQUEST_SERDE = new JsonSerde<>() {
            @Override
            protected TypeReference<CustomerTravelRequest> getTypeReference() {
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

        Serde<TimeTableUpdate> TIME_UPDATE_SERDE = new JsonSerde<>() {
            @Override
            protected TypeReference<TimeTableUpdate> getTypeReference() {
                return new TypeReference<>() {
                };
            }
        };
    }
}
