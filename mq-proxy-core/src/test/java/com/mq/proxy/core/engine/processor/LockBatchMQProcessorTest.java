package com.mq.proxy.core.engine.processor;

import com.mq.proxy.core.engine.ClientConnectionManager;
import com.mq.proxy.core.protocol.RemotingCommand;
import com.mq.proxy.core.protocol.RemotingSysResponseCode;
import com.mq.proxy.core.protocol.RequestCode;
import com.mq.proxy.core.protocol.body.LockBatchRequestBody;
import com.mq.proxy.core.protocol.body.LockBatchResponseBody;
import com.mq.proxy.core.protocol.body.MessageQueue;
import com.mq.proxy.core.protocol.body.UnlockBatchRequestBody;
import io.netty.channel.Channel;
import org.junit.Before;
import org.junit.Test;

import java.util.HashSet;
import java.util.Set;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

public class LockBatchMQProcessorTest {

    private ClientConnectionManager clientConnectionManager;
    private LockBatchMQProcessor processor;
    private Channel mockChannel;

    @Before
    public void setUp() {
        clientConnectionManager = new ClientConnectionManager();
        processor = new LockBatchMQProcessor(clientConnectionManager);
        mockChannel = mock(Channel.class);
        when(mockChannel.isActive()).thenReturn(true);
        when(mockChannel.remoteAddress()).thenReturn(null);
    }

    @Test
    public void testLockBatchMQ() throws Exception {
        LockBatchRequestBody requestBody = new LockBatchRequestBody();
        requestBody.setConsumerGroup("testGroup");
        requestBody.setClientId("client-001");
        Set<MessageQueue> mqSet = new HashSet<>();
        mqSet.add(new MessageQueue("TestTopic", "broker-a", 0));
        mqSet.add(new MessageQueue("TestTopic", "broker-a", 1));
        requestBody.setMqSet(mqSet);

        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.LOCK_BATCH_MQ, null);
        request.setBody(requestBody.encode());

        RemotingCommand response = processor.processRequest(mockChannel, request);

        assertNotNull(response);
        assertEquals(RemotingSysResponseCode.SUCCESS, response.getCode());
        assertNotNull(response.getBody());

        LockBatchResponseBody responseBody = LockBatchResponseBody.decode(response.getBody());
        assertNotNull(responseBody.getLockOKMQSet());
        assertEquals(2, responseBody.getLockOKMQSet().size());
    }

    @Test
    public void testLockRenewBySameClient() throws Exception {
        LockBatchRequestBody firstRequest = new LockBatchRequestBody();
        firstRequest.setConsumerGroup("testGroup");
        firstRequest.setClientId("client-001");
        Set<MessageQueue> mqSet = new HashSet<>();
        mqSet.add(new MessageQueue("TestTopic", "broker-a", 0));
        firstRequest.setMqSet(mqSet);

        RemotingCommand firstCmd = RemotingCommand.createRequestCommand(RequestCode.LOCK_BATCH_MQ, null);
        firstCmd.setBody(firstRequest.encode());
        processor.processRequest(mockChannel, firstCmd);

        LockBatchRequestBody secondRequest = new LockBatchRequestBody();
        secondRequest.setConsumerGroup("testGroup");
        secondRequest.setClientId("client-001");
        secondRequest.setMqSet(mqSet);

        RemotingCommand secondCmd = RemotingCommand.createRequestCommand(RequestCode.LOCK_BATCH_MQ, null);
        secondCmd.setBody(secondRequest.encode());
        RemotingCommand response = processor.processRequest(mockChannel, secondCmd);

        assertEquals(RemotingSysResponseCode.SUCCESS, response.getCode());
        LockBatchResponseBody responseBody = LockBatchResponseBody.decode(response.getBody());
        assertEquals(1, responseBody.getLockOKMQSet().size());
    }

    @Test
    public void testLockFailByDifferentClient() throws Exception {
        Channel channel2 = mock(Channel.class);
        when(channel2.isActive()).thenReturn(true);
        when(channel2.remoteAddress()).thenReturn(null);

        LockBatchRequestBody firstRequest = new LockBatchRequestBody();
        firstRequest.setConsumerGroup("testGroup");
        firstRequest.setClientId("client-001");
        Set<MessageQueue> mqSet = new HashSet<>();
        mqSet.add(new MessageQueue("TestTopic", "broker-a", 0));
        firstRequest.setMqSet(mqSet);

        RemotingCommand firstCmd = RemotingCommand.createRequestCommand(RequestCode.LOCK_BATCH_MQ, null);
        firstCmd.setBody(firstRequest.encode());
        processor.processRequest(mockChannel, firstCmd);

        LockBatchRequestBody secondRequest = new LockBatchRequestBody();
        secondRequest.setConsumerGroup("testGroup");
        secondRequest.setClientId("client-002");
        secondRequest.setMqSet(mqSet);

        RemotingCommand secondCmd = RemotingCommand.createRequestCommand(RequestCode.LOCK_BATCH_MQ, null);
        secondCmd.setBody(secondRequest.encode());
        RemotingCommand response = processor.processRequest(channel2, secondCmd);

        assertEquals(RemotingSysResponseCode.SUCCESS, response.getCode());
        LockBatchResponseBody responseBody = LockBatchResponseBody.decode(response.getBody());
        assertEquals(0, responseBody.getLockOKMQSet().size());
    }

    @Test
    public void testUnlockBatchMQ() throws Exception {
        LockBatchRequestBody lockRequest = new LockBatchRequestBody();
        lockRequest.setConsumerGroup("testGroup");
        lockRequest.setClientId("client-001");
        Set<MessageQueue> mqSet = new HashSet<>();
        mqSet.add(new MessageQueue("TestTopic", "broker-a", 0));
        mqSet.add(new MessageQueue("TestTopic", "broker-a", 1));
        lockRequest.setMqSet(mqSet);

        RemotingCommand lockCmd = RemotingCommand.createRequestCommand(RequestCode.LOCK_BATCH_MQ, null);
        lockCmd.setBody(lockRequest.encode());
        processor.processRequest(mockChannel, lockCmd);

        UnlockBatchRequestBody unlockRequest = new UnlockBatchRequestBody();
        unlockRequest.setConsumerGroup("testGroup");
        unlockRequest.setClientId("client-001");
        unlockRequest.setMqSet(mqSet);

        RemotingCommand unlockCmd = RemotingCommand.createRequestCommand(RequestCode.UNLOCK_BATCH_MQ, null);
        unlockCmd.setBody(unlockRequest.encode());
        RemotingCommand response = processor.processRequest(mockChannel, unlockCmd);

        assertNotNull(response);
        assertEquals(RemotingSysResponseCode.SUCCESS, response.getCode());

        assertTrue(processor.isLockOwned("testGroup", new MessageQueue("TestTopic", "broker-a", 0), "client-002"));
    }

    @Test
    public void testUnlockFailByDifferentClient() throws Exception {
        Channel channel2 = mock(Channel.class);
        when(channel2.isActive()).thenReturn(true);
        when(channel2.remoteAddress()).thenReturn(null);

        LockBatchRequestBody lockRequest = new LockBatchRequestBody();
        lockRequest.setConsumerGroup("testGroup");
        lockRequest.setClientId("client-001");
        Set<MessageQueue> mqSet = new HashSet<>();
        mqSet.add(new MessageQueue("TestTopic", "broker-a", 0));
        lockRequest.setMqSet(mqSet);

        RemotingCommand lockCmd = RemotingCommand.createRequestCommand(RequestCode.LOCK_BATCH_MQ, null);
        lockCmd.setBody(lockRequest.encode());
        processor.processRequest(mockChannel, lockCmd);

        assertFalse(processor.isLockOwned("testGroup", new MessageQueue("TestTopic", "broker-a", 0), "client-002"));

        UnlockBatchRequestBody unlockRequest = new UnlockBatchRequestBody();
        unlockRequest.setConsumerGroup("testGroup");
        unlockRequest.setClientId("client-002");
        unlockRequest.setMqSet(mqSet);

        RemotingCommand unlockCmd = RemotingCommand.createRequestCommand(RequestCode.UNLOCK_BATCH_MQ, null);
        unlockCmd.setBody(unlockRequest.encode());
        processor.processRequest(channel2, unlockCmd);

        assertFalse(processor.isLockOwned("testGroup", new MessageQueue("TestTopic", "broker-a", 0), "client-002"));
    }

    @Test
    public void testLockWithEmptyBody() throws Exception {
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.LOCK_BATCH_MQ, null);
        request.setBody(null);

        RemotingCommand response = processor.processRequest(mockChannel, request);

        assertEquals(RemotingSysResponseCode.SYSTEM_ERROR, response.getCode());
    }

    @Test
    public void testUnsupportedRequestCode() throws Exception {
        RemotingCommand request = RemotingCommand.createRequestCommand(999, null);
        request.setBody(new byte[0]);

        RemotingCommand response = processor.processRequest(mockChannel, request);

        assertEquals(RemotingSysResponseCode.REQUEST_CODE_NOT_SUPPORTED, response.getCode());
    }

    @Test
    public void testReleaseLocksOnChannelInactive() throws Exception {
        LockBatchRequestBody lockRequest = new LockBatchRequestBody();
        lockRequest.setConsumerGroup("testGroup");
        lockRequest.setClientId("client-001");
        Set<MessageQueue> mqSet = new HashSet<>();
        mqSet.add(new MessageQueue("TestTopic", "broker-a", 0));
        mqSet.add(new MessageQueue("TestTopic", "broker-a", 1));
        lockRequest.setMqSet(mqSet);

        RemotingCommand lockCmd = RemotingCommand.createRequestCommand(RequestCode.LOCK_BATCH_MQ, null);
        lockCmd.setBody(lockRequest.encode());
        RemotingCommand lockResponse = processor.processRequest(mockChannel, lockCmd);
        assertEquals(RemotingSysResponseCode.SUCCESS, lockResponse.getCode());

        assertFalse(processor.isLockOwned("testGroup", new MessageQueue("TestTopic", "broker-a", 0), "client-002"));

        processor.releaseLocksByChannel(mockChannel);

        assertTrue(processor.isLockOwned("testGroup", new MessageQueue("TestTopic", "broker-a", 0), "client-002"));
    }
}