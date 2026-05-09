package com.mq.proxy.core.engine.processor;

import com.mq.proxy.core.engine.MessageEngine;
import com.mq.proxy.core.protocol.RemotingCommand;
import com.mq.proxy.core.protocol.RemotingSysResponseCode;
import com.mq.proxy.core.protocol.RequestCode;
import com.mq.proxy.core.protocol.ResponseCode;
import com.mq.proxy.core.protocol.header.SendMessageRequestHeader;
import com.mq.proxy.core.protocol.header.SendMessageRequestHeaderV2;
import com.mq.proxy.core.protocol.header.SendMessageResponseHeader;
import com.mq.proxy.core.server.RemotingProcessor;
import com.mq.proxy.core.storage.model.InternalMessage;
import com.mq.proxy.core.storage.model.PutResult;
import io.netty.channel.Channel;

import java.util.HashMap;
import java.util.Map;

public class SendMessageProcessor implements RemotingProcessor {

    private final MessageEngine messageEngine;

    public SendMessageProcessor(MessageEngine messageEngine) {
        this.messageEngine = messageEngine;
    }

    @Override
    public RemotingCommand processRequest(Channel channel, RemotingCommand request) throws Exception {
        SendMessageRequestHeader requestHeader;
        int requestCode = request.getCode();

        if (requestCode == RequestCode.SEND_MESSAGE) {
            requestHeader = parseSendMessageRequestHeader(request);
        } else if (requestCode == RequestCode.SEND_MESSAGE_V2 || requestCode == RequestCode.SEND_BATCH_MESSAGE) {
            SendMessageRequestHeaderV2 v2 = parseSendMessageRequestHeaderV2(request);
            requestHeader = SendMessageRequestHeaderV2.createSendMessageRequestHeaderV1(v2);
            if (requestCode == RequestCode.SEND_BATCH_MESSAGE) {
                requestHeader.setBatch(true);
            }
        } else {
            return RemotingCommand.createResponseCommand(RemotingSysResponseCode.REQUEST_CODE_NOT_SUPPORTED, "unsupported request code");
        }

        InternalMessage message = InternalMessage.createFromSendMessageRequest(requestHeader, request.getBody());
        PutResult putResult = messageEngine.putMessage(message);

        if (putResult.isSuccess()) {
            SendMessageResponseHeader responseHeader = new SendMessageResponseHeader();
            responseHeader.setMsgId(putResult.getMsgId());
            responseHeader.setQueueId(putResult.getQueueId());
            responseHeader.setQueueOffset(putResult.getQueueOffset());
            if (putResult.getTransactionId() != null) {
                responseHeader.setTransactionId(putResult.getTransactionId());
            }
            RemotingCommand response = RemotingCommand.createResponseCommand(RemotingSysResponseCode.SUCCESS);
            response.setCustomHeader(responseHeader);
            response.makeCustomHeaderToNet();
            return response;
        } else {
            return RemotingCommand.createResponseCommand(putResult.getResponseCode(), putResult.getRemark());
        }
    }

    private SendMessageRequestHeader parseSendMessageRequestHeader(RemotingCommand request) {
        SendMessageRequestHeader header = (SendMessageRequestHeader) request.getCustomHeader();
        if (header != null) {
            return header;
        }
        header = new SendMessageRequestHeader();
        HashMap<String, String> extFields = request.getExtFields();
        if (extFields != null) {
            header.setProducerGroup(extFields.get("producerGroup"));
            header.setTopic(extFields.get("topic"));
            header.setDefaultTopic(extFields.get("defaultTopic"));
            if (extFields.get("defaultTopicQueueNums") != null) {
                header.setDefaultTopicQueueNums(Integer.parseInt(extFields.get("defaultTopicQueueNums")));
            }
            if (extFields.get("queueId") != null) {
                header.setQueueId(Integer.parseInt(extFields.get("queueId")));
            }
            if (extFields.get("sysFlag") != null) {
                header.setSysFlag(Integer.parseInt(extFields.get("sysFlag")));
            }
            if (extFields.get("bornTimestamp") != null) {
                header.setBornTimestamp(Long.parseLong(extFields.get("bornTimestamp")));
            }
            if (extFields.get("flag") != null) {
                header.setFlag(Integer.parseInt(extFields.get("flag")));
            }
            header.setProperties(extFields.get("properties"));
            if (extFields.get("reconsumeTimes") != null) {
                header.setReconsumeTimes(Integer.parseInt(extFields.get("reconsumeTimes")));
            }
            if (extFields.get("unitMode") != null) {
                header.setUnitMode(Boolean.parseBoolean(extFields.get("unitMode")));
            }
            if (extFields.get("batch") != null) {
                header.setBatch(Boolean.parseBoolean(extFields.get("batch")));
            }
            if (extFields.get("maxReconsumeTimes") != null) {
                header.setMaxReconsumeTimes(Integer.parseInt(extFields.get("maxReconsumeTimes")));
            }
            if (extFields.get("topicSysFlag") != null) {
                header.setTopicSysFlag(Integer.parseInt(extFields.get("topicSysFlag")));
            }
            if (extFields.get("bname") != null) {
                header.setBname(extFields.get("bname"));
            }
        }
        return header;
    }

    private SendMessageRequestHeaderV2 parseSendMessageRequestHeaderV2(RemotingCommand request) {
        SendMessageRequestHeaderV2 v2 = new SendMessageRequestHeaderV2();
        HashMap<String, String> extFields = request.getExtFields();
        if (extFields != null) {
            v2.setA(extFields.get("a"));
            v2.setB(extFields.get("b"));
            v2.setC(extFields.get("c"));
            if (extFields.get("d") != null) {
                v2.setD(Integer.parseInt(extFields.get("d")));
            }
            if (extFields.get("e") != null) {
                v2.setE(Integer.parseInt(extFields.get("e")));
            }
            if (extFields.get("f") != null) {
                v2.setF(Integer.parseInt(extFields.get("f")));
            }
            if (extFields.get("g") != null) {
                v2.setG(Long.parseLong(extFields.get("g")));
            }
            if (extFields.get("h") != null) {
                v2.setH(Integer.parseInt(extFields.get("h")));
            }
            v2.setI(extFields.get("i"));
            if (extFields.get("j") != null) {
                v2.setJ(Integer.parseInt(extFields.get("j")));
            }
            if (extFields.get("k") != null) {
                v2.setK(Boolean.parseBoolean(extFields.get("k")));
            }
            if (extFields.get("l") != null) {
                v2.setL(Integer.parseInt(extFields.get("l")));
            }
            if (extFields.get("m") != null) {
                v2.setM(Boolean.parseBoolean(extFields.get("m")));
            }
            v2.setN(extFields.get("n"));
        }
        return v2;
    }
}
