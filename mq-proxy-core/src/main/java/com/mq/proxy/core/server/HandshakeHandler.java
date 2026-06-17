package com.mq.proxy.core.server;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLEngine;

/**
 * 动态 TLS 检测处理器，参考 RocketMQ 原生 NettyRemotingServer.HandshakeHandler 实现。
 *
 * 工作原理：
 * 1. 偷看入站数据的第一个字节
 * 2. 如果是 {@link #HANDSHAKE_MAGIC_CODE}（0x16，TLS 握手 magic code），根据 {@link TlsMode} 决定是否动态添加 SslHandler
 * 3. 检测完成后移除自身，将消息传递给下一个 handler
 *
 * 这样同一端口可以同时支持 TLS 和非 TLS 客户端（PERMISSIVE 模式），
 * 与 RocketMQ 原生服务器的 TLS 协商机制完全兼容。
 */
@ChannelHandler.Sharable
public class HandshakeHandler extends SimpleChannelInboundHandler<ByteBuf> {

    private static final Logger log = LoggerFactory.getLogger(HandshakeHandler.class);

    /**
     * TLS 握手记录的第一个字节（ContentType: Handshake）。
     * 参见 RFC 5246 6.2.1 Fragmentation 和 RFC 8446 5.1 Record Layer。
     */
    private static final byte HANDSHAKE_MAGIC_CODE = 0x16;

    private final TlsMode tlsMode;
    private final SslContext sslContext;

    /**
     * @param tlsMode    TLS 模式
     * @param sslContext SSL 上下文，PERMISSIVE/ENFORCING 模式下不能为 null
     */
    public HandshakeHandler(TlsMode tlsMode, SslContext sslContext) {
        this.tlsMode = tlsMode;
        this.sslContext = sslContext;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, ByteBuf msg) throws Exception {
        // 数据不足时无法判断，等待更多数据
        if (!msg.isReadable() || msg.readableBytes() < 1) {
            return;
        }

        // 标记读取位置，便于偷看第一个字节后重置
        msg.markReaderIndex();
        byte firstByte = msg.getByte(0);

        boolean clientWantTls = (firstByte == HANDSHAKE_MAGIC_CODE);

        if (clientWantTls) {
            switch (tlsMode) {
                case DISABLED:
                    log.warn("Client intends to establish SSL connection while server is in TLS disabled mode, closing channel: {}",
                            ctx.channel().remoteAddress());
                    ctx.close();
                    return;
                case PERMISSIVE:
                case ENFORCING:
                    if (sslContext != null) {
                        SSLEngine sslEngine = sslContext.newEngine(ctx.alloc());
                        ctx.pipeline().addAfter("handshakeHandler", "ssl", new SslHandler(sslEngine));
                        log.info("SSL handler added to channel pipeline for TLS connection: {}", ctx.channel().remoteAddress());
                    } else {
                        log.error("Client wants SSL but sslContext is null, closing channel: {}", ctx.channel().remoteAddress());
                        ctx.close();
                        return;
                    }
                    break;
                default:
                    log.warn("Unknown TLS mode: {}", tlsMode);
                    break;
            }
        } else if (tlsMode == TlsMode.ENFORCING) {
            log.warn("Client intends to establish an insecure connection while server is in TLS enforcing mode, closing channel: {}",
                    ctx.channel().remoteAddress());
            ctx.close();
            return;
        }

        // 重置 reader index，让后续 handler（SslHandler 或 frameDecoder）能完整读取数据
        msg.resetReaderIndex();

        // 移除自身，后续消息不再经过 HandshakeHandler
        try {
            ctx.pipeline().remove(this);
        } catch (Exception e) {
            log.error("Error while removing HandshakeHandler from pipeline", e);
        }

        // 将消息传递给下一个 handler
        ctx.fireChannelRead(msg.retain());
    }
}
