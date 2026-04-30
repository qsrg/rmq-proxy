package com.mq.proxy.core.protocol;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class RemotingCommand {
    public static final int RPC_TYPE = 0;
    public static final int RPC_ONEWAY = 1;

    private static final AtomicInteger requestId = new AtomicInteger(0);

    private static final ObjectMapper MAPPER = new ObjectMapper();

    static {
        MAPPER.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);
    }

    private int code;
    private byte language = 0;
    private int version = 0;
    private int opaque;
    private int flag = 0;
    private String remark;
    private HashMap<String, String> extFields;
    private transient CommandCustomHeader customHeader;
    private transient byte[] body;
    private SerializeType serializeTypeCurrentRPC = SerializeType.JSON;

    public RemotingCommand() {
    }

    public static RemotingCommand createRequestCommand(int code, CommandCustomHeader customHeader) {
        RemotingCommand cmd = new RemotingCommand();
        cmd.setCode(code);
        cmd.customHeader = customHeader;
        cmd.setOpaque(getAndSet());
        return cmd;
    }

    public static RemotingCommand createResponseCommand(int code, String remark) {
        RemotingCommand cmd = new RemotingCommand();
        cmd.markResponseType();
        cmd.setCode(code);
        cmd.setRemark(remark);
        return cmd;
    }

    public static RemotingCommand createResponseCommand(int code) {
        return createResponseCommand(code, null);
    }

    public static int getAndSet() {
        return requestId.getAndIncrement();
    }

    public void markResponseType() {
        int bits = 1 << RPC_TYPE;
        this.flag |= bits;
    }

    @JsonIgnore
    public boolean isResponseType() {
        int bits = 1 << RPC_TYPE;
        return (this.flag & bits) == bits;
    }

    public void markOnewayRPC() {
        int bits = 1 << RPC_ONEWAY;
        this.flag |= bits;
    }

    @JsonIgnore
    public boolean isOnewayRPC() {
        int bits = 1 << RPC_ONEWAY;
        return (this.flag & bits) == bits;
    }

    public void makeCustomHeaderToNet() {
        if (this.customHeader != null) {
            Map<String, String> map = this.customHeader.toMap();
            if (map != null && !map.isEmpty()) {
                if (this.extFields == null) {
                    this.extFields = new HashMap<>();
                }
                this.extFields.putAll(map);
            }
        }
    }

    public byte[] headerEncode() {
        this.makeCustomHeaderToNet();
        switch (this.serializeTypeCurrentRPC) {
            case ROCKETMQ:
                return rocketMQHeaderEncode();
            case JSON:
            default:
                return jsonHeaderEncode();
        }
    }

    public static RemotingCommand headerDecode(byte[] headerData, SerializeType type) throws IOException {
        switch (type) {
            case ROCKETMQ:
                return rocketMQHeaderDecode(headerData);
            case JSON:
            default:
                return jsonHeaderDecode(headerData);
        }
    }

    private byte[] jsonHeaderEncode() {
        try {
            return MAPPER.writeValueAsBytes(this);
        } catch (JsonProcessingException e) {
            throw new RuntimeException("json encode error", e);
        }
    }

    private static RemotingCommand jsonHeaderDecode(byte[] headerData) throws IOException {
        return MAPPER.readValue(headerData, RemotingCommand.class);
    }

    private byte[] rocketMQHeaderEncode() {
        int remarkLen = 0;
        byte[] remarkBytes = null;
        if (this.remark != null && this.remark.length() > 0) {
            remarkBytes = this.remark.getBytes(StandardCharsets.UTF_8);
            remarkLen = remarkBytes.length;
        }

        int extFieldsLen = 0;
        byte[] extFieldsBytes = null;
        if (this.extFields != null && !this.extFields.isEmpty()) {
            extFieldsBytes = rocketMQExtFieldsEncode();
            extFieldsLen = extFieldsBytes.length;
        }

        int totalLen = 4 + 1 + 4 + 4 + 4 + (4 + remarkLen) + (4 + extFieldsLen);
        ByteBuffer buffer = ByteBuffer.allocate(totalLen);

        buffer.putInt(this.code);
        buffer.put(this.language);
        buffer.putInt(this.version);
        buffer.putInt(this.opaque);
        buffer.putInt(this.flag);

        if (remarkBytes != null) {
            buffer.putInt(remarkBytes.length);
            buffer.put(remarkBytes);
        } else {
            buffer.putInt(0);
        }

        if (extFieldsBytes != null) {
            buffer.putInt(extFieldsBytes.length);
            buffer.put(extFieldsBytes);
        } else {
            buffer.putInt(0);
        }

        return buffer.array();
    }

    private byte[] rocketMQExtFieldsEncode() {
        if (this.extFields == null || this.extFields.isEmpty()) {
            return null;
        }

        int totalLen = 4;
        for (Map.Entry<String, String> entry : this.extFields.entrySet()) {
            if (entry.getKey() != null && entry.getValue() != null) {
                byte[] keyBytes = entry.getKey().getBytes(StandardCharsets.UTF_8);
                byte[] valueBytes = entry.getValue().getBytes(StandardCharsets.UTF_8);
                totalLen += 4 + keyBytes.length + 4 + valueBytes.length;
            }
        }

        ByteBuffer buffer = ByteBuffer.allocate(totalLen);
        buffer.putInt(this.extFields.size());

        for (Map.Entry<String, String> entry : this.extFields.entrySet()) {
            if (entry.getKey() != null && entry.getValue() != null) {
                byte[] keyBytes = entry.getKey().getBytes(StandardCharsets.UTF_8);
                byte[] valueBytes = entry.getValue().getBytes(StandardCharsets.UTF_8);

                buffer.putInt(keyBytes.length);
                buffer.put(keyBytes);
                buffer.putInt(valueBytes.length);
                buffer.put(valueBytes);
            }
        }

        return buffer.array();
    }

    private static RemotingCommand rocketMQHeaderDecode(byte[] headerData) throws IOException {
        ByteBuffer buffer = ByteBuffer.wrap(headerData);
        RemotingCommand cmd = new RemotingCommand();

        cmd.code = buffer.getInt();
        cmd.language = buffer.get();
        cmd.version = buffer.getInt();
        cmd.opaque = buffer.getInt();
        cmd.flag = buffer.getInt();

        int remarkLen = buffer.getInt();
        if (remarkLen > 0) {
            byte[] remarkBytes = new byte[remarkLen];
            buffer.get(remarkBytes);
            cmd.remark = new String(remarkBytes, StandardCharsets.UTF_8);
        }

        int extFieldsLen = buffer.getInt();
        if (extFieldsLen > 0) {
            int count = buffer.getInt();
            cmd.extFields = new HashMap<>();
            for (int i = 0; i < count; i++) {
                int keyLen = buffer.getInt();
                byte[] keyBytes = new byte[keyLen];
                buffer.get(keyBytes);
                int valueLen = buffer.getInt();
                byte[] valueBytes = new byte[valueLen];
                buffer.get(valueBytes);
                cmd.extFields.put(new String(keyBytes, StandardCharsets.UTF_8), new String(valueBytes, StandardCharsets.UTF_8));
            }
        }

        return cmd;
    }

    public int getCode() {
        return code;
    }

    public void setCode(int code) {
        this.code = code;
    }

    public LanguageCode getLanguage() {
        return LanguageCode.forCode(language);
    }

    public void setLanguage(LanguageCode language) {
        this.language = language != null ? language.getCode() : 0;
    }

    public int getVersion() {
        return version;
    }

    public void setVersion(int version) {
        this.version = version;
    }

    public int getOpaque() {
        return opaque;
    }

    public void setOpaque(int opaque) {
        this.opaque = opaque;
    }

    public int getFlag() {
        return flag;
    }

    public void setFlag(int flag) {
        this.flag = flag;
    }

    public String getRemark() {
        return remark;
    }

    public void setRemark(String remark) {
        this.remark = remark;
    }

    public HashMap<String, String> getExtFields() {
        return extFields;
    }

    public void setExtFields(HashMap<String, String> extFields) {
        this.extFields = extFields;
    }

    @JsonIgnore
    public CommandCustomHeader getCustomHeader() {
        return customHeader;
    }

    public void setCustomHeader(CommandCustomHeader customHeader) {
        this.customHeader = customHeader;
    }

    @JsonIgnore
    public byte[] getBody() {
        return body;
    }

    public void setBody(byte[] body) {
        this.body = body;
    }

    public SerializeType getSerializeTypeCurrentRPC() {
        return serializeTypeCurrentRPC;
    }

    public void setSerializeTypeCurrentRPC(SerializeType serializeTypeCurrentRPC) {
        this.serializeTypeCurrentRPC = serializeTypeCurrentRPC;
    }

    @Override
    public String toString() {
        return "RemotingCommand{" +
                "code=" + code +
                ", language=" + language +
                ", version=" + version +
                ", opaque=" + opaque +
                ", flag=" + flag +
                ", remark='" + remark + '\'' +
                ", extFields=" + extFields +
                ", serializeTypeCurrentRPC=" + serializeTypeCurrentRPC +
                '}';
    }
}
