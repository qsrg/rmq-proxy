package com.mq.proxy.core.protocol.header;

import com.mq.proxy.core.protocol.CommandCustomHeader;

import java.util.HashMap;
import java.util.Map;

public class PullMessageResponseHeader implements CommandCustomHeader {
    private Long suggestWhichBrokerId;
    private Long nextBeginOffset;
    private Long minOffset;
    private Long maxOffset;

    @Override
    public void checkFields() {
    }

    @Override
    public Map<String, String> toMap() {
        Map<String, String> map = new HashMap<>();
        if (suggestWhichBrokerId != null) {
            map.put("suggestWhichBrokerId", String.valueOf(suggestWhichBrokerId));
        }
        if (nextBeginOffset != null) {
            map.put("nextBeginOffset", String.valueOf(nextBeginOffset));
        }
        if (minOffset != null) {
            map.put("minOffset", String.valueOf(minOffset));
        }
        if (maxOffset != null) {
            map.put("maxOffset", String.valueOf(maxOffset));
        }
        return map;
    }

    public Long getSuggestWhichBrokerId() {
        return suggestWhichBrokerId;
    }

    public void setSuggestWhichBrokerId(Long suggestWhichBrokerId) {
        this.suggestWhichBrokerId = suggestWhichBrokerId;
    }

    public Long getNextBeginOffset() {
        return nextBeginOffset;
    }

    public void setNextBeginOffset(Long nextBeginOffset) {
        this.nextBeginOffset = nextBeginOffset;
    }

    public Long getMinOffset() {
        return minOffset;
    }

    public void setMinOffset(Long minOffset) {
        this.minOffset = minOffset;
    }

    public Long getMaxOffset() {
        return maxOffset;
    }

    public void setMaxOffset(Long maxOffset) {
        this.maxOffset = maxOffset;
    }
}
