package org.example.ticketservice.models.services.impl;

import lombok.RequiredArgsConstructor;
import org.example.ticketservice.models.constants.TicketStatus;
import org.example.ticketservice.models.dto.requests.CreateTicketDetailRequest;
import org.example.ticketservice.models.dto.requests.CreateTicketRequest;
import org.example.ticketservice.models.dto.responses.TicketDetailResponse;
import org.example.ticketservice.models.dto.responses.TicketResponse;
import org.example.ticketservice.models.entities.Ticket;
import org.example.ticketservice.models.entities.TicketDetail;
import org.example.ticketservice.models.repositories.TicketDetailRepository;
import org.example.ticketservice.models.repositories.TicketRepository;
import org.example.ticketservice.models.services.TicketService;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;

import java.util.ArrayList;
import java.util.List;

@Service
@RequiredArgsConstructor
public class TicketServiceImpl implements TicketService {
    private final TicketRepository ticketRepository;
    private final TicketDetailRepository ticketDetailRepository;
    private final TripGatewayService tripGatewayService;
    private final KafkaTemplate<String, String> kafkaTemplate;

    @Value("${notification.kafka.order-created-topic:ticket-created}")
    private String orderCreatedTopic;

    @Override
    @Transactional
    public TicketResponse createTicket(CreateTicketRequest request) {
        List<TicketDetail> details = new ArrayList<>();
        double totalAmount = 0;

        for (CreateTicketDetailRequest item : request.items()) {
            var trip = tripGatewayService.getTripById(item.tripId());
            if (trip.ticketPrice() == null || trip.availableSeats() == null) {
                throw new IllegalStateException("Trip " + item.tripId() + " has incomplete price or seat information");
            }
            if (item.seat() <= 0 || item.seat() > trip.availableSeats()) {
                throw new IllegalArgumentException("Requested seats must be greater than 0 and no more than available seats for trip " + item.tripId());
            }

            double lineTotal = trip.ticketPrice() * item.seat();
            totalAmount += lineTotal;
            details.add(TicketDetail.builder()
                    .tripId(item.tripId())
                    .seats(item.seat())
                    .ticketPrice(trip.ticketPrice())
                    .lineTotal(lineTotal)
                    .build());
        }

        Ticket ticket = ticketRepository.save(Ticket.builder()
                .passengerName(request.passengerName())
                .passengerEmail(request.passengerEmail())
                .totalAmount(totalAmount)
                .status(TicketStatus.PENDING)
                .build());

        details.forEach(detail -> detail.setTicket(ticket));
        List<TicketDetail> savedDetails = ticketDetailRepository.saveAll(details);
        TicketResponse response = new TicketResponse(ticket.getId(), ticket.getPassengerName(),
                ticket.getPassengerEmail(), ticket.getTotalAmount(), ticket.getStatus(),
                savedDetails.stream().map(detail -> new TicketDetailResponse(detail.getId(), detail.getTripId(),
                        detail.getSeats(), detail.getTicketPrice(), detail.getLineTotal())).toList());
        publishAfterCommit(request.passengerEmail());
        return response;
    }

    @Override
    @Transactional(readOnly = true)
    public List<TicketResponse> getTickets() {
        return ticketRepository.findAll().stream()
                .map(ticket -> new TicketResponse(ticket.getId(), ticket.getPassengerName(),
                        ticket.getPassengerEmail(), ticket.getTotalAmount(), ticket.getStatus(),
                        ticketDetailRepository.findAll().stream()
                                .filter(detail -> detail.getTicket() != null
                                        && detail.getTicket().getId().equals(ticket.getId()))
                                .map(detail -> new TicketDetailResponse(detail.getId(), detail.getTripId(),
                                        detail.getSeats(), detail.getTicketPrice(), detail.getLineTotal()))
                                .toList()))
                .toList();
    }

    private void publishAfterCommit(String email) {
        TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization() {
            @Override
            public void afterCommit() {
                kafkaTemplate.send(orderCreatedTopic, email);
            }
        });
    }
}
