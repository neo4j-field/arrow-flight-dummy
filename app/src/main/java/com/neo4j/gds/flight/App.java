package com.neo4j.gds.flight;

import org.apache.arrow.flight.FlightServer;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.LocationSchemes;
import org.apache.arrow.memory.*;
import org.apache.arrow.util.AutoCloseables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;

public class App implements AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(App.class);

    private final BufferAllocator rootAllocator;
    private final FlightServer server;
    private final Location location;
    private final Producer producer;

    public App(BufferAllocator rootAllocator, Location location) throws IOException {
        this(rootAllocator, location, null, null);
    }

    public App(BufferAllocator parentAllocator, Location location, @Nullable Path cert, @Nullable Path privateKey) throws IOException {
        this.rootAllocator = parentAllocator.newChildAllocator("app", 0, Long.MAX_VALUE);
        this.producer = new Producer(rootAllocator);
        this.location = location;

        final FlightServer.Builder builder = FlightServer.builder(this.rootAllocator, this.location, this.producer);
        // TODO: auth

        if (location.getUri().getScheme().equalsIgnoreCase(LocationSchemes.GRPC_TLS)) {
            if (cert == null || privateKey == null)
                throw new IllegalArgumentException("can't use grpc+tls if missing cert or private key");
            builder.useTls(cert.toFile(), privateKey.toFile());
        }

        this.server = builder.build();
    }

    public void start() throws IOException {
        server.start();
        log.info("started FlightServer listing on {}", this.location);
    }

    public void shutdown(long timeout, TimeUnit unit) throws InterruptedException {
        log.info("shutting down FlightServer");
        server.shutdown();
        server.awaitTermination(timeout, unit);
        log.info("FlightServer terminated");
    }

    @Override
    public void close() throws Exception {
        AutoCloseables.close(producer, server, rootAllocator);
    }

    public void await() throws InterruptedException {
        server.awaitTermination();
    }

    public static void main(String ...args) throws Exception {
        // This is silly...and case-sensitive
        System.setProperty(DefaultAllocationManagerOption.ALLOCATION_MANAGER_TYPE_PROPERTY_NAME, "Netty");

        for (String arg : args)
            log.debug("arg: {}", arg);

        var location = new Location(new URI(args.length > 0 ? args[0] : "grpc://0.0.0.0:8491"));
        var config = RootAllocator
                .configBuilder()
                .maxAllocation(Long.MAX_VALUE)
                .allocationManagerFactory(NettyAllocationManager.FACTORY);
        var cert = (args.length > 1 ? Path.of(args[1]) : null);
        var key = (args.length > 2 ? Path.of(args[2]) : null);

        try (final RootAllocator allocator = new RootAllocator(config.build())) {
            var app = new App(allocator, location, cert, key);

            Runtime.getRuntime()
                    .addShutdownHook(new Thread(() -> {
                        try {
                            log.info("caught shutdown request");
                            app.shutdown(1, TimeUnit.MINUTES);
                        } catch (InterruptedException e) {
                            log.error("timed out waiting for shutdown");
                        } catch (Exception e) {
                            log.error("failed to cleanly shutdown server", e);
                        }
                    }, "shutdown-thread"));

            app.start();
            app.await();
        }
    }
}
