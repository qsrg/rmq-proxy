package com.mq.proxy.core.protocol.header;

import com.mq.proxy.core.protocol.CommandCustomHeader;

import java.util.HashMap;
import java.util.Map;

public class ViewMessageRequestHeader implements CommandCustomHeader {
    private Long offset;

    @Override
    public void checkFields() {
    }

    @Override
    public Map<String, String> toMap() {
        Map<String, String> map = new HashMap<>();
        if (offset != null) map.put("offset", String.valueOf(offset));
        return map;
    }

    public Long getOffset() { return offset; }
    public void setOffset(Long offset) { this.offset = offset; }
}