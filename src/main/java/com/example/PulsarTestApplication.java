package com.example;

import com.example.resources.TestResource;

import org.apache.pulsar.client.api.ClientBuilder;
import org.apache.pulsar.client.api.HashingScheme;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerCryptoFailureAction;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.impl.ClientBuilderImpl;
import org.apache.pulsar.client.impl.ProducerBuilderImpl;

import java.util.concurrent.TimeUnit;

import io.dropwizard.Application;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;

public class PulsarTestApplication extends Application<PulsarTestConfiguration> {
    PulsarClient client;

    public static void main(final String[] args) throws Exception {
        new PulsarTestApplication().run(args);
    }

    @Override
    public String getName() {
        return "PulsarTestApplication";
    }

    @Override
    public void initialize(final Bootstrap<PulsarTestConfiguration> bootstrap) {
        // nothing
    }

    @Override
    public void run(final PulsarTestConfiguration configuration,
            final Environment environment) {
        ClientBuilder clientBuilder = new ClientBuilderImpl();
        clientBuilder.operationTimeout(30, TimeUnit.SECONDS);
        clientBuilder.connectionsPerBroker(1);
        clientBuilder.enableTcpNoDelay(true);
        clientBuilder.allowTlsInsecureConnection(false);
        clientBuilder.enableTlsHostnameVerification(false); // could this be true?
        clientBuilder.statsInterval(60, TimeUnit.SECONDS);
        clientBuilder.maxConcurrentLookupRequests(5000);
        clientBuilder.maxLookupRequests(50000);
        clientBuilder.maxNumberOfRejectedRequestPerConnection(50);
        clientBuilder.keepAliveInterval(30, TimeUnit.SECONDS);
        clientBuilder.connectionTimeout(10, TimeUnit.SECONDS);
        clientBuilder.ioThreads(Runtime.getRuntime().availableProcessors() * 2);
        clientBuilder.listenerThreads(2);
        clientBuilder.serviceUrl("pulsar://localhost:6650");
        try {
            client = clientBuilder.build();
        } catch (PulsarClientException e) {
            e.printStackTrace();
        }
        final TestResource testResource = new TestResource(client);
        environment.jersey().register(testResource);
    }

}
