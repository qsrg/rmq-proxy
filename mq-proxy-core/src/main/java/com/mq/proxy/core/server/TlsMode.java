package com.mq.proxy.core.server;

/**
 * TLS 模式枚举，与 RocketMQ 原生 TlsMode 保持一致。
 *
 * <ul>
 *     <li><strong>DISABLED:</strong> 不支持 SSL；收到 SSL 握手将拒绝连接。</li>
 *     <li><strong>PERMISSIVE:</strong> SSL 可选，同一端口同时支持 TLS 和非 TLS 客户端。</li>
 *     <li><strong>ENFORCING:</strong> 强制 SSL，非 SSL 连接将被拒绝。</li>
 * </ul>
 */
public enum TlsMode {
    DISABLED("disabled"),
    PERMISSIVE("permissive"),
    ENFORCING("enforcing");

    private final String name;

    TlsMode(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }

    public static TlsMode parse(String name) {
        if (name == null || name.trim().isEmpty()) {
            return PERMISSIVE;
        }
        String trimmed = name.trim().toLowerCase();
        for (TlsMode mode : values()) {
            if (mode.name.equals(trimmed)) {
                return mode;
            }
        }
        return PERMISSIVE;
    }
}
