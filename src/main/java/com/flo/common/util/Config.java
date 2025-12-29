package com.flo.common.util;

import java.util.List;

public class Config {

    private static class ClusterInfo {
        private String nodeName;
        private String nodeIpAddress;
        private String nodeListeningPort;
        private int heatBeatFrequencyInterval;
        private List<String> clusterIpAddress;
    }

    private static class TaskConfig {
        private TaskType taskType;
        private static String filePath;
        private static FileType fileType;
    }

    private enum TaskType {
        SqlGenerator,
        FileSorter,
        Decoder,
        Encoder,
        MapReduce
    }

    private enum FileType {
        Text,
        Csv,
        Sql,
        Xls
    }
}
