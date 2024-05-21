package org.total_order_broadcast;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;

import java.util.List;
import java.util.ArrayList;
import java.io.IOException;

import org.total_order_broadcast.Node.JoinGroupMsg;
import org.total_order_broadcast.Node.StartMessage;
import org.total_order_broadcast.Node.WriteDataMsg;



public class Cluster {
    final static int N_NODES = 2;

    public static void main(String[] args) {

    // Create the actor system
    final ActorSystem system = ActorSystem.create("vssystem");

    // Create a "virtual synchrony coordinator" by default the coordinator is the node with max(ID)
    ActorRef coordinator = system.actorOf(Replica.props(N_NODES), "coordinator");
    
    // Create nodes and put them to a list
    List<ActorRef> group = new ArrayList<>();

    group.add(coordinator);
    
    for (int i=N_NODES-1; i>-1; i--) {
      group.add(system.actorOf(Replica.props(i), "replica-" + i));
    }

    // Send join messages to the coordinator and the nodes to inform them of the whole group
    JoinGroupMsg start = new JoinGroupMsg(group, coordinator);
    
    for (ActorRef peer: group) {
      peer.tell(start, ActorRef.noSender());
    }

    ActorRef client = system.actorOf(Client.props(), "client");
    client.tell(start, ActorRef.noSender());
    
    client.tell(new WriteDataMsg(1,client), client);
    inputContinue();

    // system shutdown
    system.terminate();
  }

  public static void inputContinue() {
    try {
      System.out.println(">>> Press ENTER to continue <<<");
      System.in.read();
    }
    catch (IOException ignored) {}
  }
}
