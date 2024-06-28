package org.total_order_broadcast;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;

import java.util.List;
import java.util.Map;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Scanner;
import java.util.concurrent.TimeUnit;

import org.slf4j.MDC;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.total_order_broadcast.Client.*;
import org.total_order_broadcast.Node.*;

/**
 * The {@code Cluster} class is the main class responsible for setting up and managing
 * a virtual synchrony cluster of nodes using Akka actors. It initializes the actor system,
 * creates nodes and clients, and handles user input to perform actions within the cluster.
 * It also includes logging functionality with MDC (Mapped Diagnostic Context) for enhanced log management.
 *
 * <p>Usage:
 * <pre>
 *   java org.total_order_broadcast.Cluster
 * </pre>
 * </p>
 *
 * <p>This class is the entry point for the application and includes the main method
 * that sets up the environment and starts the interactive user input loop.</p>
 *
 * @see akka.actor.ActorSystem
 * @see akka.actor.ActorRef
 * @see org.total_order_broadcast.Node
 * @see org.total_order_broadcast.Client
 * @see org.slf4j.MDC
 * @see org.slf4j.Logger
 * @see org.slf4j.LoggerFactory
 */
public class Cluster {
  private static Map<String, String> contextMap =  new HashMap<>();

  private static final Logger logger = LoggerFactory.getLogger(Cluster.class);

  /**
   * The main method that sets up the actor system, creates the nodes and clients,
   * and starts the interactive user input loop to perform actions within the cluster.
   *
   * @param args Command line arguments (not used).
   */
  public static void main(String[] args) {

    Scanner in = new Scanner(System.in);
    contextMap.put("replicaId", String.valueOf("system"));

    // Create the actor system
    final ActorSystem system = ActorSystem.create("vssystem");

    // Create a "virtual synchrony coordinator" by default the coordinator is the
    // node with max(ID)
    ActorRef coordinator = system.actorOf(Replica.props(Node.N_PARTICIPANTS), "coordinator");

    // Create nodes and put them to a list
    List<ActorRef> group = new ArrayList<>();
    List<ActorRef> clients = new ArrayList<>();

    group.add(coordinator);

    for (int i = Node.N_PARTICIPANTS - 1; i > 0; i--) {
      group.add(system.actorOf(Replica.props(i), "replica-" + i));
    }

    ActorRef supervisor = system.actorOf(Client.props(-1), "supervisor");

    // Send join messages to the coordinator and the nodes to inform them of the
    // whole group
    JoinGroupMsg start = new JoinGroupMsg(group, coordinator, supervisor);

    for (ActorRef peer : group) {
      peer.tell(start, ActorRef.noSender());
    }

    ActorRef client_1 = system.actorOf(Client.props(), "client_1");
    ActorRef client_2 = system.actorOf(Client.props(), "client_2");
    
    supervisor.tell(new Supervise(), ActorRef.noSender());

    clients.add(client_1);
    clients.add(client_2);

    client_1.tell(start, ActorRef.noSender());
    client_2.tell(start, ActorRef.noSender());

    try {
      TimeUnit.SECONDS.sleep(1);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }

    inputContinue(in, clients, group, supervisor);

    // system shutdown
    system.terminate();
    in.close();

  }

  public static void inputContinue(Scanner in, List<ActorRef> clients, List<ActorRef> group, ActorRef supervisor) {
    boolean exit = false;
    int updateValue = 1;
    int clientId = 0;
    int replicaId = 0;
    int input = 0;
    ActorRef replica;
    List<ActorRef> replicas = new ArrayList<>();
    ActorRef client;
    String[] clientNames = clients.stream().map(c -> c.path().name()).toArray(String[]::new);
    String[] replicaNames = group.stream().map(r -> r.path().name()).toArray(String[]::new);

    while (!exit) {

      try {
        String[] actions = {
            "Exit",
            "Update: choose a client to trigger the update",
            "Read: choose a client to read the latest value",
            "Concurrent updates",
            "Crash: choose a replica to crash",
            "Crash coordinator",
            "Update and crash: choose a client to trigger the update and crash its replica",
            "Coordinator crash in middle of updates (before acks are received)",
            "Coordinator crash in middle of updates (before request is sent to replicas)",
            "Crash replica in election coordinator",
            "Crash two replicas in election coordinator",
            "Read history",
          };

        input = readInput(in, actions);

        switch (input) {
          case 0:
            // Exit
            System.out.println("Exiting...");
            exit = true;
            break;
          case 1:
            // Client update value
            clientId = readInput(in, clientNames);
            if (clientId == -1) break;
            client = clients.get(clientId);
            client.tell(new WriteDataMsg(updateValue++, client), client);
            break;
          case 2:
            // Client read latest value
            clientId = readInput(in, clientNames);
            if (clientId == -1) break;
            client = clients.get(clientId);
            client.tell(new RequestRead(), client);
            break;
          case 3:
            // Concurrent updates
            for(ActorRef c : clients){
              c.tell(new WriteDataMsg(updateValue++, c), c);
            }
            break;
          case 4:
            // Crash replica
            replicaId = readInput(in, replicaNames);
            if (replicaId == -1) break;
            replica = group.get(replicaId);
            replica.tell(new CrashMsg(), replica);
            break;
          case 5:
            // Crash coordinator
            supervisor.tell(new CrashCoord(), supervisor);
            break;
          case 6:
            // Update and crash replica
            clientId = readInput(in, clientNames);
            if (clientId == -1) break;
            client = clients.get(clientId);
            boolean shouldCrash = true;
            client.tell(new WriteDataMsg(updateValue++, client, shouldCrash), client);
            break;
          case 7:
            //Coordinator crash in middle of updates (before request is sent to replicas)
            for(ActorRef c : clients){
              c.tell(new WriteDataMsg(updateValue++, c), c);
            }
            supervisor.tell(new CrashCoord(), supervisor);
            break;
          case 8:
            //Coordinator crash in middle of updates (before acks are received)
            for(ActorRef c : clients){
              c.tell(new WriteDataMsg(updateValue++, c), c);
            }
            Node.delay(200);
            supervisor.tell(new CrashCoord(), supervisor);
            break;
          case 9:
            //Crash replica in election coordinator
            replicaId = readInput(in, replicaNames);
            if (replicaId == -1) break;
            replica = group.get(replicaId);
            supervisor.tell(new CrashCoord(), supervisor);
            Node.delay(Node.HEARTBEAT_TIMEOUT_DURATION + 1000);
            replica.tell(new CrashMsg(), replica);
            break;
          case 10:
            //Crash two replicas in election coordinator
            for(int i = 0; i < 2; i++){
              replicaId = readInput(in, replicaNames);
              if (replicaId == -1) break;
              replicas.add(group.get(replicaId));
            }
            supervisor.tell(new CrashCoord(), supervisor);
            Node.delay(Node.HEARTBEAT_TIMEOUT_DURATION + 1000);
            replicas.forEach(r -> r.tell(new CrashMsg(), r));
            break;
          case 11:
            // Read history
            supervisor.tell(new ReadHistory(), supervisor);
            break;
          default:
            break;
        }

      } catch (Exception ignored) {
        System.err.println(ignored.getMessage());
        System.out.println("Invalid input, please try again.");
        System.exit(-1);
        input = 0;
      }

      try {
        TimeUnit.SECONDS.sleep(1);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
  }

  /**
   * Reads input from the user to select an action from a list of actions.
   *
   * @param in A {@code Scanner} object to read user input.
   * @param actions An array of strings representing the available actions.
   * @return The index of the selected action.
   */
  private static int readInput(Scanner in, String[] actions) {
    System.out.println("\n#########\n");
    System.out.println("Please select menu item");
    System.out.println(">>> Choose action <<<");
    for (int i = 0; i < actions.length; i++) {
      System.out.println("[" + i + "] - " + actions[i]);
    }

    int value = -1; // Default to an invalid value
    while (value < 0 || value >= actions.length) {
        if (in.hasNextInt()) {
            value = in.nextInt();
            if (value >= 0 && value < actions.length) {
                break; // Valid input, exit the loop
            } else {
                System.out.println("Invalid menu item, please try again");
            }
        } else {
            System.out.println("Please enter a valid integer");
            in.next(); // Consume the invalid input
        }
    }
    logWithMDC("Action chosen: [" + value + "] - " + actions[value],contextMap, logger, LogLevel.INFO);
    return value;
  }

  /**
   * Logs a message with the given context map and log level using the provided logger.
   *
   * @param message The message to be logged.
   * @param contextMap A map containing the context information for MDC.
   * @param logger The logger to be used for logging the message.
   * @param level The log level at which the message should be logged.
   */
  public static void logWithMDC(String message, Map<String, String> contextMap, Logger logger, LogLevel level) {
      try {
            // Set MDC logContext
            contextMap.forEach(MDC::put);
            // Log the message at the specified level
            switch (level) {
                case INFO:
                    logger.info(message);
                    break;
                case WARN:
                    logger.warn(message);
                    break;
                case ERROR:
                    logger.error(message);
                    break;
                case DEBUG:
                    logger.debug(message);
                    break;
            }
        } finally {
            // Clear MDC context
            MDC.clear();
        }
  }
  /**
   * Enum representing the log levels.
   */
  public enum LogLevel {
      INFO, WARN, ERROR, DEBUG
  }
}


/*
enum LogLevel {
  INFO, WARN, ERROR, DEBUG
}
 */
