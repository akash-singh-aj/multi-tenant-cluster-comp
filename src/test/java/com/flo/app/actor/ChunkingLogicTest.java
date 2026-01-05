package com.flo.app.actor;

import org.junit.Test;
import java.io.File;
import java.io.RandomAccessFile;
import java.io.FileOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class ChunkingLogicTest {

    @Test
    public void testChunkingLogicDoesNotDuplicateLines() throws Exception {
        File tempFile = File.createTempFile("chunking_test", ".csv");
        tempFile.deleteOnExit();

        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < 100; i++) {
            sb.append("300,RECORD_").append(i).append("\n");
        }
        byte[] content = sb.toString().getBytes(StandardCharsets.UTF_8);
        try (FileOutputStream fos = new FileOutputStream(tempFile)) {
            fos.write(content);
        }

        long fileSize = tempFile.length();
        long chunkSize = 50; // small chunk size to force many chunks
        long start = 0;
        
        List<long[]> chunks = new ArrayList<>();

        try (RandomAccessFile raf = new RandomAccessFile(tempFile, "r")) {
            while (start < fileSize) {
                long end = Math.min(start + chunkSize, fileSize);

                // This is the logic from WorkloadDistributor.java
                if (end < fileSize) {
                    raf.seek(end);
                    while (end < fileSize) {
                        int b = raf.read();
                        end++;
                        if (b == '\n' || b == -1) {
                            break;
                        }
                    }
                }
                chunks.add(new long[]{start, end});
                start = end;
            }
        }

        // Verify chunks are disjoint and cover the whole file
        long lastEnd = 0;
        for (long[] chunk : chunks) {
            assertEquals("Chunk start should be previous chunk end", lastEnd, chunk[0]);
            assertTrue("Chunk end should be greater than start", chunk[1] > chunk[0]);
            lastEnd = chunk[1];
        }
        assertEquals("Last chunk end should be file size", fileSize, lastEnd);

        // Verify each RECORD_i appears exactly once across all chunks
        int totalRecordsFound = 0;
        for (int i = 0; i < 100; i++) {
            String target = "300,RECORD_" + i + "\n";
            int count = 0;
            for (long[] chunk : chunks) {
                byte[] chunkData = new byte[(int)(chunk[1] - chunk[0])];
                try (RandomAccessFile raf = new RandomAccessFile(tempFile, "r")) {
                    raf.seek(chunk[0]);
                    raf.readFully(chunkData);
                }
                String chunkStr = new String(chunkData, StandardCharsets.UTF_8);
                if (chunkStr.contains(target)) {
                    count++;
                }
            }
            assertEquals("Record " + i + " should appear exactly once", 1, count);
        }
    }
    @Test
    public void testChunkingLogicDoesNotDuplicateLinesWithCRLF() throws Exception {
        File tempFile = File.createTempFile("chunking_test_crlf", ".csv");
        tempFile.deleteOnExit();

        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < 100; i++) {
            sb.append("300,RECORD_").append(i).append("\r\n");
        }
        byte[] content = sb.toString().getBytes(StandardCharsets.UTF_8);
        try (FileOutputStream fos = new FileOutputStream(tempFile)) {
            fos.write(content);
        }

        long fileSize = tempFile.length();
        long chunkSize = 50; 
        long start = 0;
        
        List<long[]> chunks = new ArrayList<>();

        try (RandomAccessFile raf = new RandomAccessFile(tempFile, "r")) {
            while (start < fileSize) {
                long end = Math.min(start + chunkSize, fileSize);

                if (end < fileSize) {
                    raf.seek(end);
                    while (end < fileSize) {
                        int b = raf.read();
                        end++;
                        if (b == '\n' || b == -1) {
                            break;
                        }
                    }
                }
                chunks.add(new long[]{start, end});
                start = end;
            }
        }

        // Verify each RECORD_i appears exactly once across all chunks
        for (int i = 0; i < 100; i++) {
            String target = "300,RECORD_" + i + "\r\n";
            int count = 0;
            for (long[] chunk : chunks) {
                byte[] chunkData = new byte[(int)(chunk[1] - chunk[0])];
                try (RandomAccessFile raf = new RandomAccessFile(tempFile, "r")) {
                    raf.seek(chunk[0]);
                    raf.readFully(chunkData);
                }
                String chunkStr = new String(chunkData, StandardCharsets.UTF_8);
                if (chunkStr.contains(target)) {
                    count++;
                }
            }
            assertEquals("Record " + i + " should appear exactly once with CRLF", 1, count);
        }
    }
}
