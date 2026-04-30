package com.mq.proxy.core.engine.processor;

import com.mq.proxy.core.engine.ClientConnectionManager;
import com.mq.proxy.core.protocol.RemotingCommand;
import com.mq.proxy.core.protocol.RemotingSysResponseCode;
import com.mq.proxy.core.protocol.RequestCode;
import com.mq.proxy.core.protocol.heartbeat.HeartbeatData;
import io.netty.channel.Channel;
import org.junit.Before;
import org.junit.Test;

import java.util.HashSet;
import java.util.Set;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

public class ClientManageProcessorTest {

    private ClientConnectionManager clientConnectionManager;
    private ClientManageProcessor processor;
    private Channel mockChannel;

    @Before
    public void setUp() {
        clientConnectionManager = new ClientConnectionManager();
        processor = new ClientManageProcessor(clientConnectionManager);
        mockChannel = mock(Channel.class);
        when(mockChannel.isActive()).thenReturn(true);
        when(mockChannel.remoteAddress()).thenReturn(null);
    }

    @Test
    public void testHeartBeat() throws Exception {
        HeartbeatData heartbeatData = new HeartbeatData();
        heartbeatData.setClientID("client-001");

        Set<HeartbeatData.ProducerData> producerDataSet = new HashSet<>();
        HeartbeatData.ProducerData producerData = new HeartbeatData.ProducerData();
        producerData.setGroupName("producerGroup1");
        producerDataSet.add(producerData);
        heartbeatData.setProducerDataSet(producerDataSet);

        Set<HeartbeatData.ConsumerData> consumerDataSet = new HashSet<>();
        HeartbeatData.ConsumerData consumerData = new HeartbeatData.ConsumerData();
        consumerData.setGroupName("consumerGroup1");
        consumerDataSet.add(consumerData);
        heartbeatData.setConsumerDataSet(consumerDataSet);

        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.HEART_BEAT, null);
        request.setBody(heartbeatData.encode());

        RemotingCommand response = processor.processRequest(mockChannel, request);

        assertNotNull(response);
        assertEquals(RemotingSysResponseCode.SUCCESS, response.getCode());

        assertTrue(clientConnectionManager.getAllProducerGroups().contains("producerGroup1"));
        assertTrue(clientConnectionManager.getAllConsumerGroups().contains("consumerGroup1"));
    }

    @Test
    public void testUnregisterClient() throws Exception {
        clientConnectionManager.registerProducer(mockChannel, "client-001", "producerGroup1");
        clientConnectionManager.registerConsumer(mockChannel, "client-001", "consumerGroup1",
                null, null, null, null);

        assertTrue(clientConnectionManager.getAllProducerGroups().contains("producerGroup1"));
        assertTrue(clientConnectionManager.getAllConsumerGroups().contains("consumerGroup1"));

        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.UNREGISTER_CLIENT, null);
        java.util.HashMap<String, String> extFields = new java.util.HashMap<>();
        extFields.put("clientID", "client-001");
        extFields.put("producerGroup", "producerGroup1");
        extFields.put("consumerGroup", "consumerGroup1");
        request.setExtFields(extFields);

        RemotingCommand response = processor.processRequest(mockChannel, request);

        assertNotNull(response);
        assertEquals(RemotingSysResponseCode.SUCCESS, response.getCode());

        assertFalse(clientConnectionManager.getAllProducerGroups().contains("producerGroup1"));
        assertFalse(clientConnectionManager.getAllConsumerGroups().contains("consumerGroup1"));
    }

    @Test
    public void testChannelInactive() throws Exception {
        clientConnectionManager.registerProducer(mockChannel, "client-001", "producerGroup1");
        clientConnectionManager.registerConsumer(mockChannel, "client-001", "consumerGroup1",
                null, null, null, null);

        assertTrue(clientConnectionManager.hasAnyClients());

        clientConnectionManager.onChannelInactive(mockChannel);

        assertFalse(clientConnectionManager.hasAnyClients());
        assertFalse(clientConnectionManager.getAllProducerGroups().contains("producerGroup1"));
        assertFalse(clientConnectionManager.getAllConsumerGroups().contains("consumerGroup1"));
    }
}
