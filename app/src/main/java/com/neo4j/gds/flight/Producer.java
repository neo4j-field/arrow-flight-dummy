package com.neo4j.gds.flight;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.arrow.flight.*;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.util.AutoCloseables;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public class Producer implements FlightProducer, AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(Producer.class);

    private static class MapTypeReference extends TypeReference<Map<String, Object>> { }
    private static final ObjectMapper mapper = new ObjectMapper();

    final BufferAllocator allocator;

    public Producer(BufferAllocator parentAllocator) {
        this.allocator = parentAllocator.newChildAllocator(this.getClass().getName(), 0, Long.MAX_VALUE);
    }

    @Override
    public void close() throws Exception {
        AutoCloseables.close(allocator);
    }

    @Override
    public void getStream(CallContext context, Ticket ticket, ServerStreamListener listener) {
        log.info("getStream: {}", ticket);
        listener.completed();
    }

    @Override
    public void listFlights(CallContext context, Criteria criteria, StreamListener<FlightInfo> listener) {
        log.info("listFlights: {}", criteria);
        listener.onCompleted();
    }

    @Override
    public FlightInfo getFlightInfo(CallContext context, FlightDescriptor descriptor) {
        log.info("getFlightInfo: {}", descriptor);
        return new FlightInfo(new Schema(List.of()), descriptor, List.of(), 0, 0);
    }

    @Override
    public Runnable acceptPut(CallContext context, FlightStream flightStream, StreamListener<PutResult> ackStream) {
        var uuid = UUID.randomUUID();

        return () -> {
            log.info("acceptPut[{}]: {}", uuid, flightStream.getDescriptor());

            try (final VectorSchemaRoot root = flightStream.getRoot()) {
                final Schema schema = root.getSchema();
                log.info("acceptPut[{}] schema: {}", uuid, schema);

                long batchCnt = 0;
                long rowCnt = 0;
                long bytes = 0;
                while (flightStream.next()) {
                    batchCnt++;
                    try (final ArrowBatch batch = ArrowBatch.fromRoot(root, allocator)) {
                        rowCnt += batch.getRowCount();
                        bytes += Arrays.stream(batch.getVectors())
                                .map(ValueVector::getBufferSize)
                                .reduce(0, Integer::sum);
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                    ackStream.onNext(PutResult.metadata(flightStream.getLatestMetadata()));
                }
                log.info("acceptPut[{}]: {} batches, {} rows, {} bytes", uuid, batchCnt, rowCnt, bytes);
            }
        };
    }

    @Override
    public void doAction(CallContext context, Action action, StreamListener<Result> listener) {
        log.info("action: {}", action.getType());

        try (final JsonParser parser = mapper.createParser(action.getBody())) {
            final Map<String, Object> body = parser.readValueAs(new MapTypeReference());
            log.info("body: {}", body);
            listener.onNext(new Result("{ \"name\": \"doAction\" }".getBytes(StandardCharsets.UTF_8)));
            listener.onCompleted();
        } catch (IOException e) {
            listener.onError(e);
        }
    }

    @Override
    public void listActions(CallContext context, StreamListener<ActionType> listener) {
        listener.onCompleted();
    }
}
