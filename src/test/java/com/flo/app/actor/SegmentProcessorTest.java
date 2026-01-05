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

public class SegmentProcessorTest {

    @ClassRule
    public static final TestKitJunitResource testKit = new TestKitJunitResource();

    @Test
    public void testOnProcessChunkWithUtf8() throws Exception {
        TestProbe<Nmi300PersistenceActor.Command> persistenceProbe = testKit.createTestProbe(Nmi300PersistenceActor.Command.class);
        ActorRef<SegmentProcessor.Command> segmentProcessor = testKit.spawn(SegmentProcessor.create(persistenceProbe.getRef()));

        File tempFile = File.createTempFile("test_utf8", ".csv");
        tempFile.deleteOnExit();

        // Line with UTF-8 character: "300,NMI_✓_123"
        String content = "300,NMI_✓_123\n";
        try (BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(tempFile), StandardCharsets.UTF_8))) {
            writer.write(content);
        }

        Path filePath = tempFile.toPath();
        segmentProcessor.tell(new SegmentProcessor.ProcessChunk(filePath, 0, tempFile.length()));

        Nmi300PersistenceActor.PersistNmi persistMsg = persistenceProbe.expectMessageClass(Nmi300PersistenceActor.PersistNmi.class);
        assertEquals("NMI_✓_123", persistMsg.nmi300().getId());

        persistenceProbe.expectMessageClass(Nmi300PersistenceActor.ChunkProcessed.class);
    }
}
