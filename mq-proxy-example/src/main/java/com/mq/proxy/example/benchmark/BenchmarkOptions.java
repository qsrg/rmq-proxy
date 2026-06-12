package com.mq.proxy.example.benchmark;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

public class BenchmarkOptions {

    public enum Target {
        PROXY, DIRECT
    }

    public enum Mode {
        PRODUCE, CONSUME, MIXED
    }

    private Target target = Target.PROXY;
    private Mode mode = Mode.MIXED;
    private String proxyAddrs;
    private String namesrvAddrs;
    private String topic = "BenchmarkTopic";
    private String producerGroup = "BenchmarkProducerGroup";
    private String consumerGroup = "BenchmarkConsumerGroup";
    private String tag = "BenchmarkTag";
    private int producerThreads = 8;
    private int consumerThreads = 4;
    private int messageSize = 1024;
    private int durationSeconds = 300;
    private int warmupSeconds = 30;
    private int reportIntervalSeconds = 10;
    private int sendTimeoutMillis = 3000;
    private String output = "target/benchmark-result.json";
    private String instanceId = "bench-" + System.currentTimeMillis();
    private boolean help;

    public static BenchmarkOptions parse(String[] args) throws ParseException {
        Options cliOptions = buildCliOptions();
        CommandLine commandLine = new DefaultParser().parse(cliOptions, args);

        BenchmarkOptions options = new BenchmarkOptions();
        options.help = commandLine.hasOption("help");
        if (options.help) {
            return options;
        }

        options.target = parseEnum(Target.class, commandLine.getOptionValue("target", "proxy"));
        options.mode = parseEnum(Mode.class, commandLine.getOptionValue("mode", "mixed"));
        options.proxyAddrs = trimToNull(commandLine.getOptionValue("proxyAddrs"));
        options.namesrvAddrs = trimToNull(commandLine.getOptionValue("namesrvAddrs"));
        options.topic = commandLine.getOptionValue("topic", options.topic);
        options.producerGroup = commandLine.getOptionValue("producerGroup", options.producerGroup);
        options.consumerGroup = commandLine.getOptionValue("consumerGroup", options.consumerGroup);
        options.tag = commandLine.getOptionValue("tag", options.tag);
        options.producerThreads = parsePositiveInt(commandLine, "producerThreads", options.producerThreads);
        options.consumerThreads = parsePositiveInt(commandLine, "consumerThreads", options.consumerThreads);
        options.messageSize = parsePositiveInt(commandLine, "messageSize", options.messageSize);
        options.durationSeconds = parsePositiveInt(commandLine, "durationSeconds", options.durationSeconds);
        options.warmupSeconds = parseNonNegativeInt(commandLine, "warmupSeconds", options.warmupSeconds);
        options.reportIntervalSeconds = parsePositiveInt(commandLine, "reportIntervalSeconds", options.reportIntervalSeconds);
        options.sendTimeoutMillis = parsePositiveInt(commandLine, "sendTimeoutMillis", options.sendTimeoutMillis);
        options.output = commandLine.getOptionValue("output", options.output);
        options.instanceId = commandLine.getOptionValue("instanceId", options.instanceId);

        options.validate();
        return options;
    }

    public static Options buildCliOptions() {
        Options options = new Options();
        options.addOption(Option.builder().longOpt("help").desc("Show help").build());
        options.addOption(Option.builder().longOpt("target").hasArg().desc("proxy or direct, default proxy").build());
        options.addOption(Option.builder().longOpt("proxyAddrs").hasArg().desc("Proxy address list, semicolon separated").build());
        options.addOption(Option.builder().longOpt("namesrvAddrs").hasArg().desc("RocketMQ NameServer address list for direct baseline").build());
        options.addOption(Option.builder().longOpt("topic").hasArg().desc("Benchmark topic").build());
        options.addOption(Option.builder().longOpt("mode").hasArg().desc("produce, consume, or mixed").build());
        options.addOption(Option.builder().longOpt("producerGroup").hasArg().desc("Producer group prefix").build());
        options.addOption(Option.builder().longOpt("consumerGroup").hasArg().desc("Consumer group").build());
        options.addOption(Option.builder().longOpt("tag").hasArg().desc("Message tag").build());
        options.addOption(Option.builder().longOpt("producerThreads").hasArg().desc("Producer worker count").build());
        options.addOption(Option.builder().longOpt("consumerThreads").hasArg().desc("Consumer consume thread count").build());
        options.addOption(Option.builder().longOpt("messageSize").hasArg().desc("Message body size in bytes").build());
        options.addOption(Option.builder().longOpt("durationSeconds").hasArg().desc("Measured duration in seconds").build());
        options.addOption(Option.builder().longOpt("warmupSeconds").hasArg().desc("Warmup duration in seconds").build());
        options.addOption(Option.builder().longOpt("reportIntervalSeconds").hasArg().desc("Console report interval in seconds").build());
        options.addOption(Option.builder().longOpt("sendTimeoutMillis").hasArg().desc("Producer send timeout in milliseconds").build());
        options.addOption(Option.builder().longOpt("output").hasArg().desc("Final JSON result path").build());
        options.addOption(Option.builder().longOpt("instanceId").hasArg().desc("Unique id for distributed benchmark process").build());
        return options;
    }

    public static void printHelp() {
        new HelpFormatter().printHelp("RocketMQProxyBenchmark", buildCliOptions(), true);
    }

    public String resolveClientNamesrvAddr() {
        return target == Target.PROXY ? proxyAddrs : namesrvAddrs;
    }

    private void validate() {
        if (target == Target.PROXY && proxyAddrs == null) {
            throw new IllegalArgumentException("proxyAddrs is required when target=proxy");
        }
        if (target == Target.DIRECT && namesrvAddrs == null) {
            throw new IllegalArgumentException("namesrvAddrs is required when target=direct");
        }
    }

    private static <E extends Enum<E>> E parseEnum(Class<E> enumClass, String value) {
        return Enum.valueOf(enumClass, value.trim().toUpperCase());
    }

    private static int parsePositiveInt(CommandLine commandLine, String name, int defaultValue) {
        int value = Integer.parseInt(commandLine.getOptionValue(name, String.valueOf(defaultValue)));
        if (value <= 0) {
            throw new IllegalArgumentException(name + " must be > 0");
        }
        return value;
    }

    private static int parseNonNegativeInt(CommandLine commandLine, String name, int defaultValue) {
        int value = Integer.parseInt(commandLine.getOptionValue(name, String.valueOf(defaultValue)));
        if (value < 0) {
            throw new IllegalArgumentException(name + " must be >= 0");
        }
        return value;
    }

    private static String trimToNull(String value) {
        if (value == null) {
            return null;
        }
        String trimmed = value.trim();
        return trimmed.isEmpty() ? null : trimmed;
    }

    public Target getTarget() {
        return target;
    }

    public Mode getMode() {
        return mode;
    }

    public String getProxyAddrs() {
        return proxyAddrs;
    }

    public String getNamesrvAddrs() {
        return namesrvAddrs;
    }

    public String getTopic() {
        return topic;
    }

    public String getProducerGroup() {
        return producerGroup;
    }

    public String getConsumerGroup() {
        return consumerGroup;
    }

    public String getTag() {
        return tag;
    }

    public int getProducerThreads() {
        return producerThreads;
    }

    public int getConsumerThreads() {
        return consumerThreads;
    }

    public int getMessageSize() {
        return messageSize;
    }

    public int getDurationSeconds() {
        return durationSeconds;
    }

    public int getWarmupSeconds() {
        return warmupSeconds;
    }

    public int getReportIntervalSeconds() {
        return reportIntervalSeconds;
    }

    public int getSendTimeoutMillis() {
        return sendTimeoutMillis;
    }

    public String getOutput() {
        return output;
    }

    public String getInstanceId() {
        return instanceId;
    }

    public boolean isHelp() {
        return help;
    }
}
