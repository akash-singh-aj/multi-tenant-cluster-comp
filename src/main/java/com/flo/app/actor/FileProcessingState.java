package com.flo.app.actor;

public class FileProcessingState {
    private int totalChunks;
    private int processedChunks;
    private boolean endOfFileReceived;

    public FileProcessingState() {
        this.totalChunks = -1; // Not known yet
        this.processedChunks = 0;
        this.endOfFileReceived = false;
    }

    public void setTotalChunks(int totalChunks) {
        this.totalChunks = totalChunks;
        this.endOfFileReceived = true;
    }

    public void incrementProcessedChunks() {
        this.processedChunks++;
    }

    public boolean isComplete() {
        return endOfFileReceived && totalChunks == processedChunks;
    }
}
