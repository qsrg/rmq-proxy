package com.mq.proxy.core.protocol.codec;

import com.mq.proxy.core.protocol.CommandCustomHeader;
import com.mq.proxy.core.protocol.LanguageCode;
import com.mq.proxy.core.protocol.RemotingCommand;
import com.mq.proxy.core.protocol.SerializeType;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.*;

public class RemotingCommandCodecTest {

    static class TestCustomHeader implements CommandCustomHeader {
        private String topic;
        private String queueId;

        public TestCustomHeader() {
        }

        public TestCustomHeader(String topic, String queueId) {
            this.topic = topic;
            this.queueId = queueId;
        }

        @Override
        public void checkFields() {
        }

        @Override
        public Map<String, String> toMap() {
            Map<String, String> map = new HashMap<>();
            if (topic != null) {
                map.put("topic", topic);
            }
            if (queueId != null) {
                map.put("queueId", queueId);
            }
            return map;
        }
    }

    @Test
    public void testEncodeDecodeWithJSON() throws Exception {
        EmbeddedChannel channel = new EmbeddedChannel(new RemotingCommandEncoder(), new RemotingCommandDecoder());

        RemotingCommand cmd = RemotingCommand.createRequestCommand(10, null);
        cmd.setLanguage(LanguageCode.JAVA);
        cmd.setVersion(1);
        cmd.setRemark("test-remark");
        cmd.setBody("hello-body".getBytes());
        HashMap<String, String> extFields = new HashMap<>();
        extFields.put("key1", "value1");
        cmd.setExtFields(extFields);
        cmd.setSerializeTypeCurrentRPC(SerializeType.JSON);

        channel.writeOutbound(cmd);

        ByteBuf encoded = channel.readOutbound();
        assertTrue(encoded != null);

        channel.writeInbound(encoded);

        RemotingCommand decoded = channel.readInbound();
        assertTrue(decoded != null);
        assertEquals(10, decoded.getCode());
        assertEquals(LanguageCode.JAVA, decoded.getLanguage());
        assertEquals("test-remark", decoded.getRemark());
        assertEquals(SerializeType.JSON, decoded.getSerializeTypeCurrentRPC());
        assertArrayEquals("hello-body".getBytes(), decoded.getBody());
        assertEquals("value1", decoded.getExtFields().get("key1"));

        channel.finish();
    }

    @Test
    public void testEncodeDecodeWithRocketMQ() throws Exception {
        EmbeddedChannel channel = new EmbeddedChannel(new RemotingCommandEncoder(), new RemotingCommandDecoder());

        RemotingCommand cmd = RemotingCommand.createRequestCommand(20, null);
        cmd.setLanguage(LanguageCode.JAVA);
        cmd.setVersion(2);
        cmd.setRemark("rmq-remark");
        cmd.setBody("rmq-body".getBytes());
        HashMap<String, String> extFields = new HashMap<>();
        extFields.put("extKey", "extValue");
        cmd.setExtFields(extFields);
        cmd.setSerializeTypeCurrentRPC(SerializeType.ROCKETMQ);

        channel.writeOutbound(cmd);

        ByteBuf encoded = channel.readOutbound();
        assertTrue(encoded != null);

        channel.writeInbound(encoded);

        RemotingCommand decoded = channel.readInbound();
        assertTrue(decoded != null);
        assertEquals(20, decoded.getCode());
        assertEquals(LanguageCode.JAVA, decoded.getLanguage());
        assertEquals(2, decoded.getVersion());
        assertEquals("rmq-remark", decoded.getRemark());
        assertEquals(SerializeType.ROCKETMQ, decoded.getSerializeTypeCurrentRPC());
        assertArrayEquals("rmq-body".getBytes(), decoded.getBody());
        assertEquals("extValue", decoded.getExtFields().get("extKey"));

        channel.finish();
    }

    @Test
    public void testFlagMethods() {
        RemotingCommand cmd = new RemotingCommand();

        assertFalse(cmd.isResponseType());
        assertFalse(cmd.isOnewayRPC());

        cmd.markResponseType();
        assertTrue(cmd.isResponseType());
        assertFalse(cmd.isOnewayRPC());

        cmd.markOnewayRPC();
        assertTrue(cmd.isResponseType());
        assertTrue(cmd.isOnewayRPC());
    }

    @Test
    public void testCreateRequestCommand() {
        RemotingCommand cmd = RemotingCommand.createRequestCommand(100, null);

        assertEquals(100, cmd.getCode());
        assertFalse(cmd.isResponseType());
        assertTrue(cmd.getOpaque() >= 0);
    }

    @Test
    public void testCreateResponseCommand() {
        RemotingCommand cmd = RemotingCommand.createResponseCommand(200, "error");

        assertEquals(200, cmd.getCode());
        assertEquals("error", cmd.getRemark());
        assertTrue(cmd.isResponseType());
    }

    @Test
    public void testCustomHeaderToNet() {
        TestCustomHeader header = new TestCustomHeader("TestTopic", "0");
        RemotingCommand cmd = RemotingCommand.createRequestCommand(300, header);

        cmd.makeCustomHeaderToNet();

        HashMap<String, String> extFields = cmd.getExtFields();
        assertTrue(extFields != null);
        assertEquals("TestTopic", extFields.get("topic"));
        assertEquals("0", extFields.get("queueId"));
    }
}
