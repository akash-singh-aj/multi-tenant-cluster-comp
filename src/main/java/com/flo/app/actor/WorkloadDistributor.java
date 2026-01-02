package com.flo.app.actor;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.SupervisorStrategy;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.PoolRouter;
import akka.actor.typed.javadsl.Receive;
import akka.actor.typed.javadsl.Routers;
import akka.actor.typed.javadsl.TimerScheduler;
import akka.cluster.typed.Cluster;
import akka.cluster.typed.Subscribe;
import akka.cluster.ClusterEvent.MemberEvent;
import akka.cluster.ClusterEvent.MemberUp;

import com.typesafe.config.Config;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.time.Duration;
import java.util.stream.Stream;

public class WorkloadDistributor extends AbstractBehavior<WorkloadDistributor.Command> {

    public interface Command {
    }


    public record StartProcessing(Path filePath,
                                  long chunkSize) implements Command {
    }

    private record ScanInputDirectory() implements Command {
    }

    public record FileProcessed(
            Path filePath) implements Command {
    }

    private record AdaptedMemberEvent(MemberEvent event) implements Command {}

    private final Path inputDir;
    private final Path processedDir;
    private final long chunkSize;
    private final ActorRef<Nmi300PersistenceActor.Command> persistenceActor;
    private final ActorRef<SegmentProcessor.Command> workerRouter;
    private final TimerScheduler<Command> timers;
    private boolean timerStarted = false;

    public static Behavior<Command> create() {
        return Behaviors.withTimers(timers -> Behaviors.setup(context -> new WorkloadDistributor(context, timers)));
    }

    private WorkloadDistributor(ActorContext<Command> context, TimerScheduler<Command> timers) {
        super(context);
        this.timers = timers;
        Config config = context.getSystem().settings().config().getConfig("file-processing");
        this.inputDir = Paths.get(config.getString("input-dir"));
        this.processedDir = Paths.get(config.getString("processed-dir"));
        this.chunkSize = config.getLong("chunk-size-mb") * 1024 * 1024;

        ActorRef<MemberEvent> memberEventAdapter = context.messageAdapter(MemberEvent.class, AdaptedMemberEvent::new);
        Cluster.get(context.getSystem()).subscriptions().tell(Subscribe.create(memberEventAdapter, MemberEvent.class));

        this.persistenceActor =
                context.spawn(Nmi300PersistenceActor.create(context.getSelf()), "persistence-actor");

        int poolSize = Runtime.getRuntime().availableProcessors();
        PoolRouter<SegmentProcessor.Command> pool = Routers.pool(
                poolSize,
                Behaviors.supervise(SegmentProcessor.create(persistenceActor)).onFailure(SupervisorStrategy.restart()));
        this.workerRouter = context.spawn(pool, "worker-pool");
    }

    @Override
    public Receive<Command> createReceive() {
        return newReceiveBuilder()
                .onMessage(AdaptedMemberEvent.class, this::onMemberEvent)
                .onMessage(ScanInputDirectory.class, this::onScanInputDirectory)
                .onMessage(StartProcessing.class, this::onStartProcessing)
                .onMessage(FileProcessed.class, this::onFileProcessed)
                .build();
    }

    private Behavior<Command> onMemberEvent(AdaptedMemberEvent adapted) {
        if (adapted.event() instanceof MemberUp && !timerStarted) {
            MemberUp memberUp = (MemberUp) adapted.event();
            if (memberUp.member().address().equals(Cluster.get(getContext().getSystem()).selfMember().address())) {
                getContext().getLog().info("This node is Up. Starting file processing timer.");
                cleanupProcessingFiles();
                timers.startTimerAtFixedRate("scan-timer", new ScanInputDirectory(), Duration.ofSeconds(5));
                timerStarted = true;
            }
        }
        return this;
    }

    private Behavior<Command> onScanInputDirectory(ScanInputDirectory command) {
        try (Stream<Path> stream = Files.list(inputDir)) {
            stream.filter(Files::isRegularFile)
                    .filter(path -> path.toString().endsWith(".csv"))
                    .forEach(file -> {
                        try {
                            Path processingFile = file.resolveSibling(file.getFileName().toString() + ".processing");
                            Files.move(file, processingFile, StandardCopyOption.ATOMIC_MOVE);
                            getContext().getLog().info("Claimed file for processing: {}", processingFile);
                            getContext().getSelf().tell(new StartProcessing(processingFile, chunkSize));
                        } catch (IOException e) {
                            getContext().getLog().debug("Could not claim file {}, another node likely processing it.", file);
                        }
                    });
        } catch (IOException e) {
            getContext().getLog().error("Failed to list files in input directory", e);
        }
        return this;
    }

    private Behavior<Command> onFileProcessed(FileProcessed command) {
        Path processingFile = command.filePath();
        String originalFileName = processingFile.getFileName().toString().replace(".processing", "");
        try {
            Path target = processedDir.resolve(originalFileName);
            Files.move(processingFile, target, StandardCopyOption.ATOMIC_MOVE);
            getContext().getLog().info("Moved processed file {} to {}", processingFile, target);
        } catch (IOException e) {
            getContext().getLog().error("Failed to move processed file {}", processingFile, e);
        }
        return this;
    }

    private Behavior<Command> onStartProcessing(StartProcessing command) {
        getContext().getLog().info("Starting to process file: {}", command.filePath());
        File file = command.filePath().toFile();

        long fileSize = file.length();
        long start = 0;
        int chunkCount = 0;

        try (RandomAccessFile raf = new RandomAccessFile(file, "r")) {
            while (start < fileSize) {
                chunkCount++;
                long end = Math.min(start + command.chunkSize, fileSize);

                // If not at the end of the file, move the end to the next newline
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

                workerRouter.tell(new SegmentProcessor.ProcessChunk(command.filePath(), start, end));
                start = end;
            }
            persistenceActor.tell(new Nmi300PersistenceActor.EndOfFile(command.filePath().toString(), chunkCount));
        } catch (Exception e) {
            getContext().getLog().error("Error splitting file", e);
        }

        return this;
    }

    private void cleanupProcessingFiles() {
        try (Stream<Path> stream = Files.list(inputDir)) {
            stream.filter(path -> path.toString().endsWith(".processing"))
                    .forEach(processingFile -> {
                        try {
                            String originalFileName = processingFile.getFileName().toString().replace(".processing", "");
                            Path originalFile = processingFile.resolveSibling(originalFileName);
                            Files.move(processingFile, originalFile, StandardCopyOption.ATOMIC_MOVE);
                            getContext().getLog().info("Retrying processing for file: {}", originalFile);
                        } catch (IOException e) {
                            getContext().getLog().error("Failed to rename processing file: {}", processingFile, e);
                        }
                    });
        } catch (IOException e) {
            getContext().getLog().error("Failed to list files in input directory for cleanup", e);
        }
    }
}
