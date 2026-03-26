package com.flo.app.actor;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;
import com.flo.app.data.Nmi300;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.RandomAccessFile;
import java.nio.channels.Channels;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;

public class SegmentProcessor extends AbstractBehavior<SegmentProcessor.Command> {

    public interface Command {
    }

    public record ProcessChunk(Path filePath, long start,
                               long end) implements Command {
    }

    private final ActorRef<Nmi300PersistenceActor.Command> persistenceActor;

    public static Behavior<Command> create(ActorRef<Nmi300PersistenceActor.Command> persistenceActor) {
        return Behaviors.setup(context -> new SegmentProcessor(context, persistenceActor));
    }

    private SegmentProcessor(ActorContext<Command> context, ActorRef<Nmi300PersistenceActor.Command> persistenceActor) {
        super(context);
        this.persistenceActor = persistenceActor;
    }

    @Override
    public Receive<Command> createReceive() {
        return newReceiveBuilder()
                .onMessage(ProcessChunk.class, this::onProcessChunk)
                .build();
    }

    private Behavior<Command> onProcessChunk(ProcessChunk command) {
        getContext().getLog().info("Processing chunk from {} to {} of file {}", command.start(), command.end(), command.filePath());

        try (RandomAccessFile raf = new RandomAccessFile(command.filePath().toFile(), "r")) {
            raf.seek(command.start());

            // Limit input stream to the chunk size
            long length = command.end() - command.start();
            try (InputStream is = Channels.newInputStream(raf.getChannel());
                 InputStream limitedIs = new BoundedInputStream(is, length);
                 BufferedReader reader = new BufferedReader(new InputStreamReader(limitedIs, StandardCharsets.UTF_8))) {

                String line;
                while ((line = reader.readLine()) != null) {
                    // Handle UTF-8 BOM if present at the start of the file
                    if (line.startsWith("\uFEFF")) {
                        line = line.substring(1);
                    }
                    if (line.startsWith("300,")) {
                        String[] parts = line.split(",");
                        if (parts.length > 1 && !parts[1].isEmpty()) {
                            Nmi300 nmi300 = new Nmi300();
                            nmi300.setId(parts[1]);
                            nmi300.setData(line);
                            persistenceActor.tell(new Nmi300PersistenceActor.PersistNmi(nmi300, command.filePath().toString()));
                        }
                    }
                }
                persistenceActor.tell(new Nmi300PersistenceActor.ChunkProcessed(command.filePath().toString()));
            }
        } catch (Exception e) {
            getContext().getLog().error("Failed to process chunk of file {}", command.filePath(), e);
            persistenceActor.tell(new Nmi300PersistenceActor.ChunkFailed(command.filePath().toString(), e));
        }

        return this;
    }

    private static class BoundedInputStream extends InputStream {
        private final InputStream in;
        private long left;

        public BoundedInputStream(InputStream in, long limit) {
            this.in = in;
            this.left = limit;
        }

        @Override
        public int read() throws java.io.IOException {
            if (left <= 0) return -1;
            int result = in.read();
            if (result != -1) left--;
            return result;
        }

        @Override
        public int read(byte[] b, int off, int len) throws java.io.IOException {
            if (len == 0) return 0;
            if (left <= 0) return -1;
            int result = in.read(b, off, (int) Math.min(len, left));
            if (result != -1) left -= result;
            return result;
        }

        @Override
        public void close() throws java.io.IOException {
            in.close();
        }
    }
}
