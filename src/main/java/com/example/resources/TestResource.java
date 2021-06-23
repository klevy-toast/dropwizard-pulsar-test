package com.example.resources;

import com.codahale.metrics.annotation.Timed;

import org.apache.pulsar.client.api.HashingScheme;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerCryptoFailureAction;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;

import java.util.List;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;

import java.nio.charset.StandardCharsets;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

@Path("/test")
@Produces(MediaType.APPLICATION_JSON)
public class TestResource {
    final int num_producers = 10;
    final PulsarClient client;
    List<Producer<byte[]>> producers;

    public TestResource(PulsarClient client) {
        this.client = client;
    }

    @GET
    @Timed
    public void sendPulsar(@QueryParam("num") int num) throws PulsarClientException {
        if (producers.isEmpty()) {
            for (int i = 0; i < num_producers; i++) {
                producers.add(client.newProducer().sendTimeout(30000, TimeUnit.MILLISECONDS).blockIfQueueFull(false).maxPendingMessages(1000)
                        .maxPendingMessagesAcrossPartitions(50000).hashingScheme(HashingScheme.JavaStringHash)
                        .cryptoFailureAction(ProducerCryptoFailureAction.FAIL).batchingMaxPublishDelay(1, TimeUnit.MILLISECONDS)
                        .batchingMaxMessages(1000).enableBatching(true).topic("persistent://public/default/test1").create());
            }

        }
        for (int i = 0; i < num; i++) {
            producers.get(i % num_producers).sendAsync(UUID.randomUUID().toString().getBytes(StandardCharsets.UTF_8));
        }
    }
}