package com.flo.app.actor;

import akka.actor.testkit.typed.javadsl.TestKitJunitResource;
import akka.actor.testkit.typed.javadsl.TestProbe;
import akka.actor.typed.ActorRef;
import org.junit.ClassRule;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Collections;

import static org.junit.Assert.assertEquals;

public class SegmentProcessorTest {

    @ClassRule
    public static final TestKitJunitResource testKit = new TestKitJunitResource();

    @Test
    public void testOnProcessChunk() throws IOException {
        TestProbe<Nmi300PersistenceActor.Command> probe = testKit.createTestProbe();
        ActorRef<SegmentProcessor.Command> worker = testKit.spawn(SegmentProcessor.create(probe.getRef()));

        Path tempFile = Files.createTempFile("test", ".csv");
        String content = "300,NMI1,DATA1\n400,OTHER,DATA\n300,NMI2,DATA2\n";
        Files.write(tempFile, content.getBytes(), StandardOpenOption.WRITE);

        worker.tell(new SegmentProcessor.ProcessChunk(tempFile, 0, tempFile.toFile().length()));

        Nmi300PersistenceActor.PersistNmi m1 = probe.expectMessageClass(Nmi300PersistenceActor.PersistNmi.class);
        assertEquals("NMI1", m1.nmi300().getId());

        Nmi300PersistenceActor.PersistNmi m2 = probe.expectMessageClass(Nmi300PersistenceActor.PersistNmi.class);
        assertEquals("NMI2", m2.nmi300().getId());

        Nmi300PersistenceActor.ChunkProcessed m3 = probe.expectMessageClass(Nmi300PersistenceActor.ChunkProcessed.class);
        assertEquals(tempFile.toString(), m3.inputFilePath());

        Files.deleteIfExists(tempFile);
    }

    @Test
    public void testOnProcessChunkBoundary() throws IOException {
        TestProbe<Nmi300PersistenceActor.Command> probe = testKit.createTestProbe();
        ActorRef<SegmentProcessor.Command> worker = testKit.spawn(SegmentProcessor.create(probe.getRef()));

        Path tempFile = Files.createTempFile("testBoundary", ".csv");
        // Each line is 15 bytes including \n
        String line1 = "300,NMI1,DATA1\n"; 
        String line2 = "300,NMI2,DATA2\n";
        Files.write(tempFile, (line1 + line2).getBytes(), StandardOpenOption.WRITE);

        // Chunk 1: Exactly line 1
        worker.tell(new SegmentProcessor.ProcessChunk(tempFile, 0, 15));
        probe.expectMessageClass(Nmi300PersistenceActor.PersistNmi.class);
        probe.expectMessageClass(Nmi300PersistenceActor.ChunkProcessed.class);

        // Chunk 2: Exactly line 2
        worker.tell(new SegmentProcessor.ProcessChunk(tempFile, 15, 30));
        probe.expectMessageClass(Nmi300PersistenceActor.PersistNmi.class);
        probe.expectMessageClass(Nmi300PersistenceActor.ChunkProcessed.class);

        Files.deleteIfExists(tempFile);
    }

    @Test
    public void testOnProcessChunkWithBOM() throws IOException {
        TestProbe<Nmi300PersistenceActor.Command> probe = testKit.createTestProbe();
        ActorRef<SegmentProcessor.Command> worker = testKit.spawn(SegmentProcessor.create(probe.getRef()));

        Path tempFile = Files.createTempFile("testBOM", ".csv");
        // UTF-8 BOM is EF BB BF
        byte[] bom = new byte[]{(byte) 0xEF, (byte) 0xBB, (byte) 0xBF};
        String content = "300,NMI1,DATA1\n";
        byte[] contentBytes = content.getBytes();
        byte[] allBytes = new byte[bom.length + contentBytes.length];
        System.arraycopy(bom, 0, allBytes, 0, bom.length);
        System.arraycopy(contentBytes, 0, allBytes, bom.length, contentBytes.length);
        
        Files.write(tempFile, allBytes, StandardOpenOption.WRITE);

        worker.tell(new SegmentProcessor.ProcessChunk(tempFile, 0, tempFile.toFile().length()));

        // Now it SHOULD see "300,"
        probe.expectMessageClass(Nmi300PersistenceActor.PersistNmi.class);
        probe.expectMessageClass(Nmi300PersistenceActor.ChunkProcessed.class);

        Files.deleteIfExists(tempFile);
    }
}
