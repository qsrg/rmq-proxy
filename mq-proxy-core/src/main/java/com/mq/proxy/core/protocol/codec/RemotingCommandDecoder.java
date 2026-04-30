package com.mq.proxy.core.protocol.codec;

import com.mq.proxy.core.protocol.RemotingCommand;
import com.mq.proxy.core.protocol.SerializeType;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;

import java.util.List;

public class RemotingCommandDecoder extends ByteToMessageDecoder {

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {
        System.out.println("[DEBUG] Decoder: readableBytes=" + in.readableBytes());
        if (in.readableBytes() < 8) {
            return;
        }

        in.markReaderIndex();
        
        int totalLength = in.readInt();
        System.out.println("[DEBUG] Decoder: totalLength=" + totalLength);
        
        if (in.readableBytes() < totalLength) {
            in.resetReaderIndex();
            return;
        }

        int oriHeaderLen = in.readInt();
        System.out.println("[DEBUG] Decoder: oriHeaderLen=" + oriHeaderLen);

        SerializeType serializeType = SerializeType.forCode((byte) ((oriHeaderLen >> 24) & 0xFF));
        int headerLength = oriHeaderLen & 0x00FFFFFF;

        if (in.readableBytes() < headerLength) {
            in.resetReaderIndex();
            return;
        }

        byte[] headerData = new byte[headerLength];
        in.readBytes(headerData);
        System.out.println("[DEBUG] Header raw bytes (first 200): " + new String(headerData, 0, Math.min(200, headerData.length), java.nio.charset.StandardCharsets.UTF_8));

        int bodyLength = totalLength - 4 - headerLength;
        byte[] bodyData = null;
        if (bodyLength > 0) {
            bodyData = new byte[bodyLength];
            in.readBytes(bodyData);
        }

        RemotingCommand cmd = RemotingCommand.headerDecode(headerData, serializeType);
        cmd.setBody(bodyData);
        cmd.setSerializeTypeCurrentRPC(serializeType);
        System.out.println("[DEBUG] Decoder decoded command: code=" + cmd.getCode() + ", opaque=" + cmd.getOpaque());

        out.add(cmd);
    }
}
