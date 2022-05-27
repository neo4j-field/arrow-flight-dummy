package com.neo4j.gds.flight;

import org.apache.arrow.flight.FlightServer;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.LocationSchemes;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.util.AutoCloseables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.IOException;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;

public class App implements AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(App.class);

    private final BufferAllocator rootAllocator;
    private final Location location;
    private final FlightServer server;
    private final Producer producer;

    public App(BufferAllocator rootAllocator, Location location) throws IOException {
        this(rootAllocator, location, null, null);
    }

    public App(BufferAllocator rootAllocator, Location location, @Nullable Path cert, @Nullable Path privateKey) throws IOException {
        this.rootAllocator = rootAllocator;
        this.location = location;

        this.producer = new Producer(rootAllocator);

        final FlightServer.Builder builder = FlightServer.builder(rootAllocator, location, producer);
        // TODO: auth

        if (location.getUri().getScheme().equalsIgnoreCase(LocationSchemes.GRPC_TLS)) {
            assert privateKey != null;
            assert cert != null;
            builder.useTls(cert.toFile(), privateKey.toFile());
        }

        this.server = builder.build();
    }

    public void start() throws IOException {
        server.start();
        log.info("started FlightServer");
    }

    public void shutdown(long timeout, TimeUnit unit) throws InterruptedException {
        server.shutdown();
        server.awaitTermination(timeout, unit);
    }

    @Override
    public void close() throws Exception {
        AutoCloseables.close(producer, server, rootAllocator);
    }

    public static void main(String[] args) throws IOException {
        log.info("Starting...");
        final App app = new App(
                new RootAllocator(Long.MAX_VALUE),
                Location.forGrpcInsecure("localhost", 8491));

        Runtime.getRuntime()
                .addShutdownHook(new Thread(() -> {
                    try {
                        app.shutdown(1, TimeUnit.MINUTES);
                    } catch (InterruptedException e) {
                        log.error("timed out waiting for shutdown");
                    } catch (Exception e) {
                        log.error("failed to cleanly shutdown server", e);
                    }
                }));

        app.start();
    }
}
