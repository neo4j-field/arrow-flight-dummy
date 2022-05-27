package com.neo4j.gds.flight;

import org.apache.arrow.flight.*;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.util.AutoCloseables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Producer implements FlightProducer, AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(Producer.class);

    final BufferAllocator allocator;

    public Producer(BufferAllocator parentAllocator) {
        this.allocator = parentAllocator.newChildAllocator(this.getClass().getName(), 0, Long.MAX_VALUE);
    }

    @Override
    public void close() throws Exception {
        AutoCloseables.closeNoChecked(allocator);
    }

    @Override
    public void getStream(CallContext context, Ticket ticket, ServerStreamListener listener) {

    }

    @Override
    public void listFlights(CallContext context, Criteria criteria, StreamListener<FlightInfo> listener) {

    }

    @Override
    public FlightInfo getFlightInfo(CallContext context, FlightDescriptor descriptor) {
        return null;
    }

    @Override
    public Runnable acceptPut(CallContext context, FlightStream flightStream, StreamListener<PutResult> ackStream) {
        return null;
    }

    @Override
    public void doAction(CallContext context, Action action, StreamListener<Result> listener) {

    }

    @Override
    public void listActions(CallContext context, StreamListener<ActionType> listener) {

    }
}
