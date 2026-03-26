package com.flo.app;

import akka.actor.typed.ActorSystem;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.Behaviors;
import akka.cluster.typed.Cluster;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.flo.app.actor.WorkloadDistributor;

public class Main {
    private static final Logger log = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) {
        Behavior<Void> rootBehavior = Behaviors.setup(context -> {
            context.spawn(WorkloadDistributor.create(), "WorkloadDistributor");
            return Behaviors.empty();
        });

        ActorSystem<Void> system = ActorSystem.create(rootBehavior, "ClusterSystem");
        Cluster cluster = Cluster.get(system);
        log.info("Starting mt cluster");
        log.info("Akka Cluster started: {}", cluster.selfMember());
        log.info(">>> Application started. Press Ctrl-C to exit. <<<");

        // Add a shutdown hook to gracefully terminate the actor system and wait for completion
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            system.terminate();
            system.getWhenTerminated().toCompletableFuture().join();
        }));
    }
}
