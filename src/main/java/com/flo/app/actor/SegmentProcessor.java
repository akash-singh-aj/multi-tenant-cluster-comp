package com.flo.app.actor;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;
import com.flo.app.data.Nmi300;

import java.io.RandomAccessFile;
import java.nio.file.Path;

public class SegmentProcessor extends AbstractBehavior<SegmentProcessor.Command> {

    public interface Command {
    }

    public record ProcessChunk(Path filePath, long start, long end) implements Command {}

    private record ProcessChunkWithRetry(ProcessChunk originalCommand, int retries) implements Command {}

    private static final int MAX_RETRIES = 3;

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
                .onMessage(ProcessChunk.class, command -> onProcessChunk(new ProcessChunkWithRetry(command, 0)))
                .onMessage(ProcessChunkWithRetry.class, this::onProcessChunk)
                .build();
    }

    private Behavior<Command> onProcessChunk(ProcessChunkWithRetry command) {
        ProcessChunk originalCommand = command.originalCommand();
        getContext().getLog().info("Processing chunk from {} to {} of file {}", originalCommand.start(), originalCommand.end(), originalCommand.filePath());

        try (RandomAccessFile file = new RandomAccessFile(originalCommand.filePath().toFile(), "r")) {
            file.seek(originalCommand.start());
            long currentPos = originalCommand.start();

            while (currentPos < originalCommand.end()) {
                String line = file.readLine();
                if (line == null) break;
                currentPos = file.getFilePointer();

                if (line.startsWith("300")) {
                    String[] parts = line.split(",");
                    if (parts.length > 1) {
                        Nmi300 nmi300 = new Nmi300();
                        nmi300.setId(parts[1]);
                        persistenceActor.tell(new Nmi300PersistenceActor.PersistNmi(nmi300, originalCommand.filePath().toString()));
                    }
                }
            }
            persistenceActor.tell(new Nmi300PersistenceActor.ChunkProcessed(originalCommand.filePath().toString()));
        } catch (Exception e) {
            getContext().getLog().error("Failed to process chunk after {} retries", command.retries(), e);
            if (command.retries() < MAX_RETRIES) {
                getContext().getSelf().tell(new ProcessChunkWithRetry(originalCommand, command.retries() + 1));
            } else {
                persistenceActor.tell(new Nmi300PersistenceActor.ChunkFailed(originalCommand.filePath().toString(), e));
            }
        }

        return this;
    }
}
