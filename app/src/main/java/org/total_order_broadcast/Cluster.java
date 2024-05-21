package org.total_order_broadcast;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;

import java.util.List;
import java.util.ArrayList;
import java.io.IOException;

import org.total_order_broadcast.Client.RequestRead;
import org.total_order_broadcast.Node.JoinGroupMsg;
import org.total_order_broadcast.Node.WriteDataMsg;
import org.total_order_broadcast.Node;

public class Cluster {

  public static void main(String[] args) {

    // Create the actor system
    final ActorSystem system = ActorSystem.create("vssystem");

    // Create a "virtual synchrony coordinator" by default the coordinator is the
    // node with max(ID)
    ActorRef coordinator = system.actorOf(Replica.props(Node.N_PARTICIPANTS), "coordinator");

    // Create nodes and put them to a list
    List<ActorRef> group = new ArrayList<>();

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
    client_1.tell(start, ActorRef.noSender());

    ActorRef client_2 = system.actorOf(Client.props(), "client_2");
    client_2.tell(start, ActorRef.noSender());

    for (int i = 1; i < 3; i++) {
      client_1.tell(new WriteDataMsg(i, client_1), client_1);
      client_2.tell(new RequestRead(), client_2);
    }
    
    inputContinue();
    client_2.tell(new RequestRead(), client_2);
    client_1.tell(new RequestRead(), client_1);

    // system shutdown
    system.terminate();
  }

  public static void inputContinue() {
    try {
      System.out.println(">>> Press ENTER to continue <<<");
      System.in.read();
    } catch (IOException ignored) {
    }
  }
}
