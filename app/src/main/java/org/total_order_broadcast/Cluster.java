package org.total_order_broadcast;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;

import java.util.List;
import java.util.Map;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Scanner;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import org.slf4j.MDC;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.total_order_broadcast.Client.RequestRead;
import org.total_order_broadcast.Node.CrashMsg;
import org.total_order_broadcast.Node.JoinGroupMsg;
import org.total_order_broadcast.Node.WriteDataMsg;

public class Cluster {
  private static Map<String, String> contextMap =  new HashMap<>();

  private static final Logger logger = LoggerFactory.getLogger(Cluster.class);
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

    // Send join messages to the coordinator and the nodes to inform them of the
    // whole group
    JoinGroupMsg start = new JoinGroupMsg(group, coordinator);

    for (ActorRef peer : group) {
      peer.tell(start, ActorRef.noSender());
    }

    ActorRef client_1 = system.actorOf(Client.props(), "client_1");
    ActorRef client_2 = system.actorOf(Client.props(), "client_2");
    clients.add(client_1);
    clients.add(client_2);

    client_1.tell(start, ActorRef.noSender());

    client_2.tell(start, ActorRef.noSender());

    try {
      TimeUnit.SECONDS.sleep(1);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }

    inputContinue(in, clients, group);

    // system shutdown
    system.terminate();
    in.close();

  }

  public static void inputContinue(Scanner in, List<ActorRef> clients, List<ActorRef> group) {
    boolean exit = false;
    int updateValue = 1;
    int clientId = 0;
    int replicaId = 0;
    int input = 0;
    ActorRef replica;
    ActorRef client;
    String[] clientNames = clients.stream().map(c -> c.path().name()).toArray(String[]::new);
    String[] replicaNames = group.stream().map(r -> r.path().name()).toArray(String[]::new);

    while (!exit) {

      try {
        String[] actions = {
            "Exit",
            "Update",
            "Read",
            "Crash",
            "Crash coordinator",
            "Update and crash",
            "Concurrent updates"
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
            // Crash replica
            replicaId = readInput(in, replicaNames);
            if (replicaId == -1) break;
            replica = group.get(replicaId);
            replica.tell(new CrashMsg(), replica);
            break;
          case 4:
            // Crash coordinator
            // replicas forward to the coordinator
            for (ActorRef node : group) {
              node.tell(new Node.CrashCoord(), node);
            }
            break;
          case 5:
            // Update and crash
            clientId = readInput(in, clientNames);
            if (clientId == -1) break;
            client = clients.get(clientId);
            boolean shouldCrash = true;
            client.tell(new WriteDataMsg(updateValue++, client, shouldCrash), client);
            break;
            case 6:
            // Concurrent updates
            int clientId1 = readInput(in, clientNames);
            String[] filteredClientNames = Stream.of(clientNames).filter(name -> !name.equals(clientNames[clientId1])).toArray(String[]::new);
            int clientId2 = readInput(in, filteredClientNames);
            if (clientId1 == -1 || clientId2 == -1) break;
            client = clients.get(clientId1);
            ActorRef client2 = clients.get(clientId2);
            client.tell(new WriteDataMsg(updateValue++, client), client);
            client2.tell(new WriteDataMsg(updateValue++, client2), client2);
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
}

// Define LogLevel as an enum for convenience
enum LogLevel {
  INFO, WARN, ERROR, DEBUG
}