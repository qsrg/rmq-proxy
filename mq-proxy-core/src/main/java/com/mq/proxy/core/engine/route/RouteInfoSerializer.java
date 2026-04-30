package com.mq.proxy.core.engine.route;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.core.JsonParser;
import com.mq.proxy.core.storage.model.TopicRouteInfo;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.regex.Pattern;

public class RouteInfoSerializer {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private static final Pattern UNQUOTED_NUMERIC_KEY = Pattern.compile("(\\{|,)(\\d+)\\s*:");

    static {
        MAPPER.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        MAPPER.configure(JsonParser.Feature.ALLOW_UNQUOTED_FIELD_NAMES, true);
        MAPPER.configure(JsonParser.Feature.ALLOW_SINGLE_QUOTES, true);
    }

    public static byte[] encodeTopicRouteInfo(TopicRouteInfo routeInfo) {
        try {
            return MAPPER.writeValueAsBytes(routeInfo);
        } catch (Exception e) {
            throw new RuntimeException("encode TopicRouteInfo error", e);
        }
    }

    public static TopicRouteInfo decodeTopicRouteInfo(byte[] data) {
        try {
            String json = new String(data, StandardCharsets.UTF_8);
            json = fixNumericKeys(json);
            return MAPPER.readValue(json, TopicRouteInfo.class);
        } catch (IOException e) {
            throw new RuntimeException("decode TopicRouteInfo error", e);
        }
    }

    public static String fixNumericKeys(String json) {
        return UNQUOTED_NUMERIC_KEY.matcher(json).replaceAll("$1\"$2\":");
    }
}
