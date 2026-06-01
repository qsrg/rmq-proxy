package com.mq.proxy.rocketmq.integration;

import com.mq.proxy.core.server.NettyClientConfig;
import com.mq.proxy.core.server.NettyRemotingClient;
import com.mq.proxy.core.server.NettyServerConfig;

import java.io.File;

class EmbeddedTlsRocketMQProxy {

    private final EmbeddedRocketMQProxy delegate;
    private final String certPath;
    private final String keyPath;

    private NettyRemotingClient tlsClient;

    EmbeddedTlsRocketMQProxy(String namesrvAddr, String certPath, String keyPath) {
        this.delegate = new EmbeddedRocketMQProxy(namesrvAddr) {
            @Override
            protected void configureServer(NettyServerConfig serverConfig) {
                serverConfig.setTlsEnabled(true);
                serverConfig.setTlsCertPath(certPath);
                serverConfig.setTlsKeyPath(keyPath);
            }
        };
        this.certPath = certPath;
        this.keyPath = keyPath;
    }

    void start() throws Exception {
        generateCertificates();
        delegate.start();

        NettyClientConfig tlsClientConfig = new NettyClientConfig();
        tlsClientConfig.setTlsEnabled(true);
        tlsClientConfig.setTlsTrustCertPath(certPath);
        tlsClient = new NettyRemotingClient(tlsClientConfig);
        tlsClient.start();
    }

    void shutdown() {
        if (tlsClient != null) {
            tlsClient.shutdown();
        }
        delegate.shutdown();
        deleteFile(certPath);
        deleteFile(keyPath);
    }

    String getProxyAddr() {
        return delegate.getProxyAddr();
    }

    String getBrokerAddr() {
        return delegate.getBrokerAddr();
    }

    NettyRemotingClient getTlsClient() {
        return tlsClient;
    }
    private void generateCertificates() throws Exception {
        ProcessBuilder pb = new ProcessBuilder(
                "openssl", "req", "-x509", "-newkey", "rsa:2048",
                "-keyout", keyPath, "-out", certPath, "-days", "1", "-nodes",
                "-subj", "/CN=proxy-test/OU=test/O=test/L=test/ST=test/C=CN"
        );
        pb.inheritIO();
        Process process = pb.start();
        int exitCode = process.waitFor();
        if (exitCode != 0) {
            throw new IllegalStateException("openssl cert generation failed, exitCode=" + exitCode);
        }
    }

    private void deleteFile(String path) {
        File file = new File(path);
        if (file.exists()) {
            file.delete();
        }
    }
}
