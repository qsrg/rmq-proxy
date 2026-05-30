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
        if (in.readableBytes() < 8) {
            return;
        }

        in.markReaderIndex();

        int totalLength = in.readInt();

        if (in.readableBytes() < totalLength) {
            in.resetReaderIndex();
            return;
        }

        int oriHeaderLen = in.readInt();

        SerializeType serializeType = SerializeType.forCode((byte) ((oriHeaderLen >> 24) & 0xFF));
        int headerLength = oriHeaderLen & 0x00FFFFFF;

        if (in.readableBytes() < headerLength) {
            in.resetReaderIndex();
            return;
        }

        byte[] headerData = new byte[headerLength];
        in.readBytes(headerData);

        int bodyLength = totalLength - 4 - headerLength;
        byte[] bodyData = null;
        if (bodyLength > 0) {
            bodyData = new byte[bodyLength];
            in.readBytes(bodyData);
        }

        RemotingCommand cmd = RemotingCommand.headerDecode(headerData, serializeType);
        cmd.setBody(bodyData);
        cmd.setSerializeTypeCurrentRPC(serializeType);

        out.add(cmd);
    }
}
