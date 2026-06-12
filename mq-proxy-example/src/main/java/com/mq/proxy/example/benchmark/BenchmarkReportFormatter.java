package com.mq.proxy.example.benchmark;

import java.util.Locale;

public class BenchmarkReportFormatter {

    public static String toConsoleLine(BenchmarkMetrics.Snapshot snapshot, double elapsedSeconds) {
        return String.format(Locale.US,
                "elapsed=%.1fs sendTps=%.2f sendOk=%d sendFail=%d sendErr=%.4f sendRtMs[p50=%d,p90=%d,p99=%d,max=%d] "
                        + "consumeTps=%.2f consumeOk=%d consumeFail=%d dup=%d consumeLagMs[p50=%d,p90=%d,p99=%d,max=%d]",
                elapsedSeconds,
                snapshot.getSendSuccessTps(),
                snapshot.getSendSuccess(),
                snapshot.getSendFailure(),
                snapshot.getSendErrorRate(),
                snapshot.getSendLatency().getP50(),
                snapshot.getSendLatency().getP90(),
                snapshot.getSendLatency().getP99(),
                snapshot.getSendLatency().getMax(),
                snapshot.getConsumeSuccessTps(),
                snapshot.getConsumeSuccess(),
                snapshot.getConsumeFailure(),
                snapshot.getDuplicateMessages(),
                snapshot.getConsumeLag().getP50(),
                snapshot.getConsumeLag().getP90(),
                snapshot.getConsumeLag().getP99(),
                snapshot.getConsumeLag().getMax());
    }

    public static String toJson(BenchmarkOptions options, BenchmarkMetrics.Snapshot snapshot, double elapsedSeconds) {
        StringBuilder sb = new StringBuilder(2048);
        sb.append("{\n");
        appendString(sb, "target", options.getTarget().name(), true);
        appendString(sb, "mode", options.getMode().name(), true);
        appendString(sb, "clientNamesrvAddr", options.resolveClientNamesrvAddr(), true);
        appendString(sb, "proxyAddrs", options.getProxyAddrs(), true);
        appendString(sb, "namesrvAddrs", options.getNamesrvAddrs(), true);
        appendString(sb, "topic", options.getTopic(), true);
        appendString(sb, "producerGroup", options.getProducerGroup(), true);
        appendString(sb, "consumerGroup", options.getConsumerGroup(), true);
        appendString(sb, "instanceId", options.getInstanceId(), true);
        appendNumber(sb, "elapsedSeconds", elapsedSeconds, true);
        appendNumber(sb, "messageSize", options.getMessageSize(), true);
        appendNumber(sb, "producerThreads", options.getProducerThreads(), true);
        appendNumber(sb, "consumerThreads", options.getConsumerThreads(), true);
        appendNumber(sb, "sendSuccess", snapshot.getSendSuccess(), true);
        appendNumber(sb, "sendFailure", snapshot.getSendFailure(), true);
        appendNumber(sb, "sendSuccessTps", snapshot.getSendSuccessTps(), true);
        appendNumber(sb, "sendErrorRate", snapshot.getSendErrorRate(), true);
        appendLatency(sb, "sendLatencyMs", snapshot.getSendLatency(), true);
        appendNumber(sb, "consumeSuccess", snapshot.getConsumeSuccess(), true);
        appendNumber(sb, "consumeFailure", snapshot.getConsumeFailure(), true);
        appendNumber(sb, "consumeSuccessTps", snapshot.getConsumeSuccessTps(), true);
        appendNumber(sb, "consumeErrorRate", snapshot.getConsumeErrorRate(), true);
        appendNumber(sb, "duplicateMessages", snapshot.getDuplicateMessages(), true);
        appendLatency(sb, "consumeLatencyMs", snapshot.getConsumeLatency(), true);
        appendLatency(sb, "consumeLagMs", snapshot.getConsumeLag(), false);
        sb.append("}\n");
        return sb.toString();
    }

    private static void appendLatency(StringBuilder sb, String name,
                                      BenchmarkMetrics.Snapshot.LatencySnapshot latency, boolean comma) {
        sb.append("  \"").append(name).append("\": {");
        sb.append("\"count\":").append(latency.getCount()).append(',');
        sb.append("\"avg\":").append(latency.getAvg()).append(',');
        sb.append("\"p50\":").append(latency.getP50()).append(',');
        sb.append("\"p90\":").append(latency.getP90()).append(',');
        sb.append("\"p99\":").append(latency.getP99()).append(',');
        sb.append("\"max\":").append(latency.getMax()).append(',');
        sb.append("\"overflow\":").append(latency.getOverflow());
        sb.append('}');
        if (comma) {
            sb.append(',');
        }
        sb.append('\n');
    }

    private static void appendString(StringBuilder sb, String name, String value, boolean comma) {
        sb.append("  \"").append(name).append("\":");
        if (value == null) {
            sb.append("null");
        } else {
            sb.append('"').append(escape(value)).append('"');
        }
        if (comma) {
            sb.append(',');
        }
        sb.append('\n');
    }

    private static void appendNumber(StringBuilder sb, String name, long value, boolean comma) {
        sb.append("  \"").append(name).append("\":").append(value);
        if (comma) {
            sb.append(',');
        }
        sb.append('\n');
    }

    private static void appendNumber(StringBuilder sb, String name, double value, boolean comma) {
        sb.append("  \"").append(name).append("\":")
                .append(String.format(Locale.US, "%.4f", value));
        if (comma) {
            sb.append(',');
        }
        sb.append('\n');
    }

    private static String escape(String value) {
        return value.replace("\\", "\\\\").replace("\"", "\\\"");
    }
}
