package org.total_order_broadcast;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;

import java.util.List;
import java.util.ArrayList;
import java.io.IOException;

import org.total_order_broadcast.Client.RequestRead;
import org.total_order_broadcast.Node.CrashMsg;
import org.total_order_broadcast.Node.JoinGroupMsg;
import org.total_order_broadcast.Node.WriteDataMsg;
import java.util.Scanner;
import java.util.concurrent.TimeUnit;

public class Cluster {

  public static void main(String[] args) {
    
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

    // for (int i = 1; i < 3; i++) {
    //   client_1.tell(new WriteDataMsg(i, client_1), client_1);
    //   client_2.tell(new RequestRead(), client_2);
    // }
    try {
      TimeUnit.SECONDS.sleep(1);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }

    inputContinue(clients, group);
    // client_2.tell(new RequestRead(), client_2);
    // client_1.tell(new RequestRead(), client_1);

    // system shutdown
    system.terminate();
  }

  public static void inputContinue(List<ActorRef> clients, List<ActorRef> group) {
    boolean exit = false;
    int updateValue = 1;
    int clientId = 0;
    int replicaId = 0;
    int input = 0;
    ActorRef replica;
    ActorRef client;
    String[] clientNames = clients.stream().map(c -> c.path().name()).toArray(String[]::new);
    String[] replicaNames = group.stream().map(node -> node.path().name()).toArray(String[]::new);
    Scanner in = new Scanner(System.in);

    while(!exit){

      try {
        String[] actions = {
          "Exit", 
          "Update", 
          "Read", 
          "Crash", 
          "Crash coordinator", 
          "Update and crash"};
    
        input = readInput(in,actions);
          
        switch (input) {
          case 0:
            System.out.println("Exiting...");
            System.exit(0);
            break;
          case 1:
            clientId = readInput(in,clientNames);
            client = clients.get(clientId);
            client.tell(new WriteDataMsg(updateValue++, client), client);
            break;
          case 2:
            clientId = readInput(in,clientNames);
            client = clients.get(clientId);
            client.tell(new RequestRead(), client);
            break;
          case 3:
            replicaId = readInput(in,replicaNames);
            replica = group.get(replicaId);
            replica.tell(new CrashMsg(), replica);
            break;
          case 4:
            group.get(0).tell(new CrashMsg(), group.get(0));
            break;
          case 5:
            clientId = readInput(in,clientNames);
            client = clients.get(clientId);
            boolean shouldCrash = true;
            client.tell(new WriteDataMsg(updateValue++, client, shouldCrash), client);
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
    in.close();
  }

  private static int readInput(Scanner in, String[] actions) {
    System.out.println("\n#########\n");  
    System.out.println("Please select menu item");
    System.out.println(">>> Choose action <<<");

    for (int i = 0; i < actions.length; i++) {
      System.out.println("["+ i + "] - " + actions[i]);
    }    
    
    try {
      int value = in.nextInt();
    
      while (value < 0 || value > actions.length - 1) {
        System.out.println("Invalid menu item, please try again");
        // java.util.InputMismatchException should also be caught 
        // to intercept non-numeric input
        value = in.nextInt();
      }

      return value; 
    } catch (Exception e) {
      System.err.println(e.getMessage());
    } 
    return -1;
  }
}
