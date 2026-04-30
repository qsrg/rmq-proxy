package com.mq.proxy.core.protocol;

public enum SerializeType {
    JSON((byte) 0),
    ROCKETMQ((byte) 1);

    private byte code;

    SerializeType(byte code) {
        this.code = code;
    }

    public static SerializeType forCode(byte code) {
        for (SerializeType type : SerializeType.values()) {
            if (type.getCode() == code) {
                return type;
            }
        }
        return null;
    }

    public byte getCode() {
        return code;
    }
}
