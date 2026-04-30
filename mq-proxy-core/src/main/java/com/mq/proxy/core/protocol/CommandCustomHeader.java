package com.mq.proxy.core.protocol;

import java.util.Map;

public interface CommandCustomHeader {
    void checkFields();

    Map<String, String> toMap();
}
