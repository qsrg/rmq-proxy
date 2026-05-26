package com.mq.proxy.core.engine;

import com.mq.proxy.core.protocol.heartbeat.HeartbeatData;
import io.netty.channel.Channel;
import org.junit.Before;
import org.junit.Test;

import java.util.HashSet;
import java.util.Set;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

/**
 * 广播消费模式（BROADCASTING）单元测试
 *
 * 测试覆盖：
 * 1. HeartbeatData.ConsumerData 消费模式字段
 * 2. ClientConnectionManager per-group 消费模式存储
 * 3. isBroadcastGroup 判断
 * 4. getAllConsumerData 携带消费模式
 * 5. registerConsumer/unregisterConsumer 的 per-group 逻辑
 */
public class BroadcastModeTest {

    private ClientConnectionManager manager;
    private Channel mockChannel1;
    private Channel mockChannel2;

    @Before
    public void setUp() {
        manager = new ClientConnectionManager();
        mockChannel1 = mock(Channel.class);
        mockChannel2 = mock(Channel.class);
        when(mockChannel1.isActive()).thenReturn(true);
        when(mockChannel2.isActive()).thenReturn(true);
        when(mockChannel1.remoteAddress()).thenReturn(new java.net.InetSocketAddress("127.0.0.1", 1001));
        when(mockChannel2.remoteAddress()).thenReturn(new java.net.InetSocketAddress("127.0.0.1", 1002));
    }

    // ========== HeartbeatData.ConsumerData 测试 ==========

    @Test
    public void testConsumerDataMessageModelFields() {
        HeartbeatData.ConsumerData data = new HeartbeatData.ConsumerData();
        data.setGroupName("TestGroup");
        data.setMessageModel("BROADCASTING");
        data.setConsumeType("CONSUME_PASSIVELY");
        data.setConsumeFromWhere("CONSUME_FROM_LAST_OFFSET");

        assertEquals("TestGroup", data.getGroupName());
        assertEquals("BROADCASTING", data.getMessageModel());
        assertEquals("CONSUME_PASSIVELY", data.getConsumeType());
        assertEquals("CONSUME_FROM_LAST_OFFSET", data.getConsumeFromWhere());
    }

    @Test
    public void testConsumerDataClusteringModel() {
        HeartbeatData.ConsumerData data = new HeartbeatData.ConsumerData();
        data.setGroupName("ClusterGroup");
        data.setMessageModel("CLUSTERING");

        assertEquals("CLUSTERING", data.getMessageModel());
    }

    @Test
    public void testConsumerDataDefaultNullFields() {
        HeartbeatData.ConsumerData data = new HeartbeatData.ConsumerData();
        assertNull(data.getMessageModel());
        assertNull(data.getConsumeType());
        assertNull(data.getConsumeFromWhere());
    }

    @Test
    public void testConsumerDataSerializeDeserializeWithMessageModel() {
        HeartbeatData heartbeatData = new HeartbeatData();
        heartbeatData.setClientID("testClient");

        HeartbeatData.ConsumerData consumerData = new HeartbeatData.ConsumerData();
        consumerData.setGroupName("BroadcastGroup");
        consumerData.setMessageModel("BROADCASTING");
        consumerData.setConsumeType("CONSUME_PASSIVELY");
        consumerData.setConsumeFromWhere("CONSUME_FROM_LAST_OFFSET");

        HeartbeatData.SubscriptionData sub = new HeartbeatData.SubscriptionData();
        sub.setTopic("TestTopic");
        sub.setSubString("*");
        Set<HeartbeatData.SubscriptionData> subs = new HashSet<>();
        subs.add(sub);
        consumerData.setSubscriptionDataSet(subs);

        heartbeatData.getConsumerDataSet().add(consumerData);

        // 编码/解码
        byte[] encoded = heartbeatData.encode();
        HeartbeatData decoded = HeartbeatData.decode(encoded);

        assertEquals(1, decoded.getConsumerDataSet().size());
        HeartbeatData.ConsumerData decodedData = decoded.getConsumerDataSet().iterator().next();
        assertEquals("BroadcastGroup", decodedData.getGroupName());
        assertEquals("BROADCASTING", decodedData.getMessageModel());
        assertEquals("CONSUME_PASSIVELY", decodedData.getConsumeType());
    }

    // ========== ClientConnectionManager per-group 消费模式存储 ==========

    @Test
    public void testRegisterConsumerWithBroadcastModel() {
        manager.registerConsumer(mockChannel1, "client1", "BroadcastGroup",
                "CONSUME_PASSIVELY", "BROADCASTING", "CONSUME_FROM_LAST_OFFSET",
                null);

        assertTrue(manager.isBroadcastGroup("BroadcastGroup"));
        assertFalse(manager.isBroadcastGroup("NonExistGroup"));
    }

    @Test
    public void testRegisterConsumerWithClusteringModel() {
        manager.registerConsumer(mockChannel1, "client1", "ClusterGroup",
                "CONSUME_PASSIVELY", "CLUSTERING", "CONSUME_FROM_LAST_OFFSET",
                null);

        assertFalse(manager.isBroadcastGroup("ClusterGroup"));
    }

    @Test
    public void testIsBroadcastGroupWithMixedModels() {
        // 同一个channel注册两个组：一个广播、一个集群
        manager.registerConsumer(mockChannel1, "client1", "BroadcastGroup",
                "CONSUME_PASSIVELY", "BROADCASTING", "CONSUME_FROM_LAST_OFFSET", null);
        manager.registerConsumer(mockChannel1, "client1", "ClusterGroup",
                "CONSUME_PASSIVELY", "CLUSTERING", "CONSUME_FROM_LAST_OFFSET", null);

        assertTrue(manager.isBroadcastGroup("BroadcastGroup"));
        assertFalse(manager.isBroadcastGroup("ClusterGroup"));
    }

    @Test
    public void testPerGroupMessageModelOverride() {
        // 同一个client先注册广播，后注册集群，两组模式不同
        manager.registerConsumer(mockChannel1, "client1", "GroupA",
                null, "BROADCASTING", null, null);
        manager.registerConsumer(mockChannel1, "client1", "GroupB",
                null, "CLUSTERING", null, null);

        // 全局默认值被最后一次非null设置覆盖
        // 但per-group是精确的
        ClientConnectionManager.ClientInfo clientInfo = manager.getAllClientInfos().get(0);
        assertEquals("BROADCASTING", clientInfo.getGroupMessageModel("GroupA"));
        assertEquals("CLUSTERING", clientInfo.getGroupMessageModel("GroupB"));
    }

    @Test
    public void testUnregisterRemovesGroupMetadata() {
        manager.registerConsumer(mockChannel1, "client1", "BroadcastGroup",
                "CONSUME_PASSIVELY", "BROADCASTING", "CONSUME_FROM_LAST_OFFSET", null);

        assertTrue(manager.isBroadcastGroup("BroadcastGroup"));

        manager.unregisterClient(mockChannel1, "client1", null, "BroadcastGroup");

        assertFalse(manager.isBroadcastGroup("BroadcastGroup"));
    }

    @Test
    public void testGetAllConsumerDataIncludesMessageModel() {
        manager.registerConsumer(mockChannel1, "client1", "BroadcastGroup",
                "CONSUME_PASSIVELY", "BROADCASTING", "CONSUME_FROM_LAST_OFFSET", null);
        manager.registerConsumer(mockChannel1, "client1", "ClusterGroup",
                "CONSUME_PASSIVELY", "CLUSTERING", "CONSUME_FROM_LAST_OFFSET", null);

        java.util.List<HeartbeatData.ConsumerData> consumerDataList = manager.getAllConsumerData();

        assertEquals(2, consumerDataList.size());

        for (HeartbeatData.ConsumerData data : consumerDataList) {
            if ("BroadcastGroup".equals(data.getGroupName())) {
                assertEquals("BROADCASTING", data.getMessageModel());
                assertEquals("CONSUME_PASSIVELY", data.getConsumeType());
            } else if ("ClusterGroup".equals(data.getGroupName())) {
                assertEquals("CLUSTERING", data.getMessageModel());
                assertEquals("CONSUME_PASSIVELY", data.getConsumeType());
            }
        }
    }

    @Test
    public void testNullMessageModelDefaultsToClustering() {
        manager.registerConsumer(mockChannel1, "client1", "DefaultGroup",
                null, null, null, null);

        assertFalse(manager.isBroadcastGroup("DefaultGroup"));

        ClientConnectionManager.ClientInfo clientInfo = manager.getAllClientInfos().get(0);
        // null传入后，per-group映射取默认值"CLUSTERING"
        assertEquals("CLUSTERING", clientInfo.getGroupMessageModel("DefaultGroup"));
    }

    @Test
    public void testMultipleClientsSameBroadcastGroup() {
        // 两个客户端注册同一个广播组
        manager.registerConsumer(mockChannel1, "client1", "BroadcastGroup",
                null, "BROADCASTING", null, null);
        manager.registerConsumer(mockChannel2, "client2", "BroadcastGroup",
                null, "BROADCASTING", null, null);

        assertTrue(manager.isBroadcastGroup("BroadcastGroup"));
        // 同一个组名注册两个client，getAllConsumerGroups返回去重的组集合
        assertEquals(1, manager.getAllConsumerGroups().size());
        assertEquals(2, manager.getChannelCount());  // 两个client连接
    }

    @Test
    public void testIsBroadcastGroupAfterChannelInactive() {
        manager.registerConsumer(mockChannel1, "client1", "BroadcastGroup",
                null, "BROADCASTING", null, null);

        assertTrue(manager.isBroadcastGroup("BroadcastGroup"));

        // channel断开后移除
        manager.onChannelInactive(mockChannel1);

        assertFalse(manager.isBroadcastGroup("BroadcastGroup"));
    }
}