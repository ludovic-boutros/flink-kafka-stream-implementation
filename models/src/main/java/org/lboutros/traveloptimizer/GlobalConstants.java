package org.lboutros.traveloptimizer;

public interface GlobalConstants {

    interface Topics {
        String CUSTOMER_TRAVEL_REQUEST_TOPIC = "customerTravelRequested";
        String PLANE_TIME_UPDATE_TOPIC = "planeTimeUpdated";
        String TRAIN_TIME_UPDATE_TOPIC = "trainTimeUpdated";
        String DEPARTURE_TOPIC = "departed";
        String TRAVEL_ALERTS_TOPIC = "travelChanged";
    }
}
