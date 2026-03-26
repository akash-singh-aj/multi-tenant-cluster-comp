package com.flo.app.actor;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;
import akka.actor.typed.javadsl.StashBuffer;

import com.flo.app.data.Nmi300;
import com.typesafe.config.Config;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

public class Nmi300PersistenceActor extends AbstractBehavior<Nmi300PersistenceActor.Command> {

    public interface Command {
    }


    public record PersistNmi(Nmi300 nmi300,
                             String inputFilePath) implements Command {
    }

    public record EndOfFile(String inputFilePath,
                            int totalChunks) implements Command {
    }

    public record ChunkProcessed(
            String inputFilePath) implements Command {
    }

    public record ChunkFailed(
            String inputFilePath, Throwable cause) implements Command {
    }


    private final ActorRef<WorkloadDistributor.Command> workloadDistributor;
    private final Path outputDir;
    private final Path errorDir;
    private final Map<String, ActorRef<FilePersistenceActor.Command>> fileWriters = new HashMap<>();
    private final Map<String, FileProcessingState> fileStates = new HashMap<>();
    private final StashBuffer<Command> stashBuffer;

    public static Behavior<Command> create(ActorRef<WorkloadDistributor.Command> workloadDistributor) {
        return Behaviors.withStash(1000, stash ->
                Behaviors.setup(context -> new Nmi300PersistenceActor(context, workloadDistributor, stash)));
    }

    private Nmi300PersistenceActor(ActorContext<Command> context, ActorRef<WorkloadDistributor.Command> workloadDistributor, StashBuffer<Command> stashBuffer) throws IOException {
        super(context);
        this.workloadDistributor = workloadDistributor;
        this.stashBuffer = stashBuffer;
        Config config = context.getSystem().settings().config().getConfig("file-processing");
        this.outputDir = Paths.get(config.getString("output-dir"));
        this.errorDir = Paths.get(config.getString("error-dir"));
        Files.createDirectories(this.outputDir);
        Files.createDirectories(this.errorDir);
    }

    @Override
    public Receive<Command> createReceive() {
        return idle();
    }

    private Receive<Command> idle() {
        return newReceiveBuilder()
                .onMessage(PersistNmi.class, this::onPersistNmi)
                .onMessage(EndOfFile.class, this::onEndOfFile)
                .onMessage(ChunkProcessed.class, this::onChunkProcessed)
                .onMessage(ChunkFailed.class, this::onChunkFailed)
                .build();
    }

    private Behavior<Command> creating(String inputFilePath) {
        String outputFileName = Paths.get(inputFilePath).getFileName().toString().replaceAll("\\.processing$", "") + ".sql";
        Path outputPath = outputDir.resolve(outputFileName);
        try {
            ActorRef<FilePersistenceActor.Command> newWriterActor = getContext().spawn(
                    FilePersistenceActor.create(outputPath), "file-writer-" + outputFileName);
            fileWriters.put(inputFilePath, newWriterActor);
        } catch (Exception e) {
            getContext().getLog().error("Failed to spawn FilePersistenceActor for {}, dropping stashed messages", inputFilePath, e);
            stashBuffer.unstashAll(idle());
            return idle();
        }
        return stashBuffer.unstashAll(idle());
    }


    private Behavior<Command> onPersistNmi(PersistNmi command) {
        ActorRef<FilePersistenceActor.Command> writerActor = fileWriters.get(command.inputFilePath());
        if (writerActor == null) {
            // Actor doesn't exist, stash the current message and switch to the creating behavior.
            stashBuffer.stash(command);
            return creating(command.inputFilePath());
        } else {
            // Actor exists, forward the message.
            writerActor.tell(new FilePersistenceActor.Persist(command.nmi300()));
            return this;
        }
    }


    private Behavior<Command> onEndOfFile(EndOfFile command) {
        FileProcessingState state = fileStates.computeIfAbsent(command.inputFilePath(), k -> new FileProcessingState());
        state.setTotalChunks(command.totalChunks());
        if (state.isComplete()) {
            finalizeProcessing(command.inputFilePath());
        }
        return this;
    }

    private Behavior<Command> onChunkProcessed(ChunkProcessed command) {
        FileProcessingState state = fileStates.computeIfAbsent(command.inputFilePath(), k -> new FileProcessingState());
        state.incrementProcessedChunks();
        if (state.isComplete()) {
            finalizeProcessing(command.inputFilePath());
        }
        return this;
    }

    private void finalizeProcessing(String inputFilePath) {
        ActorRef<FilePersistenceActor.Command> writerActor = fileWriters.remove(inputFilePath);
        if (writerActor != null) {
            writerActor.tell(new FilePersistenceActor.Close());
        }
        workloadDistributor.tell(new WorkloadDistributor.FileProcessed(Paths.get(inputFilePath)));
        fileStates.remove(inputFilePath);
        getContext().getLog().info("File processing complete for {}", inputFilePath);
    }

    private Behavior<Command> onChunkFailed(ChunkFailed command) {
        getContext().getLog().error("Chunk processing failed for file: {}. Moving to error directory.", command.inputFilePath(), command.cause());

        // Clean up resources
        ActorRef<FilePersistenceActor.Command> writerActor = fileWriters.remove(command.inputFilePath());
        if (writerActor != null) {
            writerActor.tell(new FilePersistenceActor.Close());
        }
        fileStates.remove(command.inputFilePath());

        // Move to error directory
        try {
            Path processingFile = Paths.get(command.inputFilePath());
            String originalFileName = processingFile.getFileName().toString().replaceAll("\\.processing$", "");
            Path target = errorDir.resolve(originalFileName);
            Files.move(processingFile, target, StandardCopyOption.ATOMIC_MOVE);
            getContext().getLog().info("Moved failed file {} to {}", processingFile, target);
        } catch (IOException e) {
            getContext().getLog().error("Failed to move failed file {}", command.inputFilePath(), e);
        }

        return this;
    }
}
