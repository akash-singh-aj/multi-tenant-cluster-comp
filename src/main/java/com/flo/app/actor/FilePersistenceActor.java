package com.flo.app.actor;

import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;
import com.flo.app.data.Nmi300;
import com.flo.app.datastore.InMemoryDataStore;

import java.io.IOException;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

public class FilePersistenceActor extends AbstractBehavior<FilePersistenceActor.Command> {

    public interface Command {}

    public record Persist(Nmi300 nmi300) implements Command {}
    public record Close() implements Command {}

    private final PrintWriter writer;

    public static Behavior<Command> create(Path outputPath) {
        return Behaviors.setup(context -> new FilePersistenceActor(context, outputPath));
    }

    private FilePersistenceActor(ActorContext<Command> context, Path outputPath) throws IOException {
        super(context);
        Files.deleteIfExists(outputPath);
        this.writer = new PrintWriter(Files.newBufferedWriter(outputPath, StandardCharsets.UTF_8));
    }

    @Override
    public Receive<Command> createReceive() {
        return newReceiveBuilder()
                .onMessage(Persist.class, this::onPersist)
                .onMessage(Close.class, this::onClose)
                .build();
    }

    private Behavior<Command> onPersist(Persist command) {
        String sql = String.format("INSERT INTO nmi300 (id, data) VALUES ('%s', '%s');\n", 
                command.nmi300().getId(), command.nmi300().getData());
        writer.println(sql);
        InMemoryDataStore.addNmi300(command.nmi300());
        return this;
    }

    private Behavior<Command> onClose(Close command) {
        writer.close();
        return Behaviors.stopped();
    }
}
