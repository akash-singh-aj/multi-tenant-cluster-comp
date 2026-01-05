package com.flo.app.actor;

import akka.actor.testkit.typed.javadsl.TestKitJunitResource;
import akka.actor.testkit.typed.javadsl.TestProbe;
import akka.actor.typed.ActorRef;
import com.flo.app.data.Nmi300;
import org.junit.ClassRule;
import org.junit.Test;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class BoundaryIssueTest {

    @ClassRule
    public static final TestKitJunitResource testKit = new TestKitJunitResource();

    @Test
    public void testInexactRecordTypeMatch() throws Exception {
        TestProbe<Nmi300PersistenceActor.Command> persistenceProbe = testKit.createTestProbe(Nmi300PersistenceActor.Command.class);
        ActorRef<SegmentProcessor.Command> segmentProcessor = testKit.spawn(SegmentProcessor.create(persistenceProbe.getRef()));

        File tempFile = File.createTempFile("test_inexact", ".csv");
        tempFile.deleteOnExit();

        // Line starting with 3000 should NOT be matched as 300
        String content = "3000,SHOULD_NOT_BE_MATCHED\n300,VALID_NMI\n";
        try (BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(tempFile), StandardCharsets.UTF_8))) {
            writer.write(content);
        }

        segmentProcessor.tell(new SegmentProcessor.ProcessChunk(tempFile.toPath(), 0, tempFile.length()));

        // We only expect ONE PersistNmi for "VALID_NMI"
        Nmi300PersistenceActor.PersistNmi persistMsg = persistenceProbe.expectMessageClass(Nmi300PersistenceActor.PersistNmi.class);
        assertEquals("VALID_NMI", persistMsg.nmi300().getId());

        persistenceProbe.expectMessageClass(Nmi300PersistenceActor.ChunkProcessed.class);
        persistenceProbe.expectNoMessage();
    }

    @Test
    public void testContextLossAcrossChunks() throws Exception {
        // This test simulates a 200 record in one chunk and a 300 record in the next.
        // Currently, SegmentProcessor doesn't handle 200 records at all.
        // If we want it to support NEM format, it should.
        
        TestProbe<Nmi300PersistenceActor.Command> persistenceProbe = testKit.createTestProbe(Nmi300PersistenceActor.Command.class);
        ActorRef<SegmentProcessor.Command> segmentProcessor = testKit.spawn(SegmentProcessor.create(persistenceProbe.getRef()));

        File tempFile = File.createTempFile("test_context", ".csv");
        tempFile.deleteOnExit();

        // 200 line contains NMI, 300 line contains data
        String content = "200,NMI12345\n300,20240101,10.5\n";
        try (BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(tempFile), StandardCharsets.UTF_8))) {
            writer.write(content);
        }

        // Simulate Chunk 1 ending after 200 line (offset 13)
        // Offset 13 is the '3' of '300'
        segmentProcessor.tell(new SegmentProcessor.ProcessChunk(tempFile.toPath(), 13, tempFile.length()));

        Nmi300PersistenceActor.PersistNmi persistMsg = persistenceProbe.expectMessageClass(Nmi300PersistenceActor.PersistNmi.class);
        // If context is maintained, ID should be NMI12345
        // Currently, it will likely be "20240101" because it takes parts[1] of 300 line.
        assertEquals("NMI12345", persistMsg.nmi300().getId());
    }
}
