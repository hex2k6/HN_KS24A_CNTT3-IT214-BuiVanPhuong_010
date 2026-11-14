package org.example.ticketservice.models.services.impl;

import lombok.RequiredArgsConstructor;
import org.example.ticketservice.clients.TripClient;
import org.example.ticketservice.exceptions.TripNotFoundException;
import org.example.ticketservice.exceptions.TripServiceException;
import org.example.ticketservice.models.dto.responses.TripResponse;
import org.springframework.cloud.client.circuitbreaker.CircuitBreakerFactory;
import feign.FeignException;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
public class TripGatewayService {
    private final TripClient tripClient;
    private final CircuitBreakerFactory circuitBreakerFactory;

    public TripResponse getTripById(Long tripId) {
        try {
            TripResponse trip = circuitBreakerFactory.create("tripService").run(
                    () -> tripClient.getTripById(tripId));
            if (trip == null || trip.id() == null || !tripId.equals(trip.id())
                    || trip.ticketPrice() == null || trip.ticketPrice() < 0
                    || trip.availableSeats() == null || trip.availableSeats() < 0) {
                throw new TripServiceException("Trip service returned invalid data for trip " + tripId);
            }
            return trip;
        } catch (TripNotFoundException | TripServiceException exception) {
            throw exception;
        } catch (FeignException.NotFound exception) {
            throw new TripNotFoundException(tripId);
        } catch (RuntimeException exception) {
            if (exception instanceof FeignException.NotFound) {
                throw new TripNotFoundException(tripId);
            }
            throw new TripServiceException("Unable to retrieve trip " + tripId, exception);
        }
    }

}
