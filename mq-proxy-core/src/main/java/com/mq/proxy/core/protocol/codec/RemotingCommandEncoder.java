package com.mq.proxy.core.protocol.codec;

import com.mq.proxy.core.protocol.RemotingCommand;
import com.mq.proxy.core.protocol.SerializeType;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;

public class RemotingCommandEncoder extends MessageToByteEncoder<RemotingCommand> {

    @Override
    protected void encode(ChannelHandlerContext ctx, RemotingCommand cmd, ByteBuf out) throws Exception {
        byte[] headerData = cmd.headerEncode();

        int headerLength = (cmd.getSerializeTypeCurrentRPC().getCode() << 24) | (headerData.length & 0x00FFFFFF);

        byte[] bodyData = cmd.getBody();
        int bodyLength = bodyData != null ? bodyData.length : 0;

        int totalLength = 4 + headerData.length + bodyLength;

        out.writeInt(totalLength);
        out.writeInt(headerLength);
        out.writeBytes(headerData);

        if (bodyData != null) {
            out.writeBytes(bodyData);
        }
    }
}
