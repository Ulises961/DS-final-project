/*
 * This Java source file was generated by the Gradle 'init' task.
 */
package org.total_order_broadcast;

import akka.actor.ActorRef;
import akka.actor.Cancellable;
import akka.actor.Props;
import java.io.Serializable;
import java.util.HashSet;
import java.util.Iterator;

public class Client extends Node {

    private ActorRef server = null;
    private HashSet<ActorRef> participants;
    public static int nextReplica = 0;
    public static int clientNumber = N_PARTICIPANTS + 1;
    private Cancellable readTimeout;
    

    public Client(){
        super(clientNumber++);
        this.participants = new HashSet<>();
    }

    static public Props props() {
        return Props.create(Client.class, Client::new);
    }
    public static class UpdateRequest implements Serializable{}

    
    public void onStartMessage(JoinGroupMsg msg) {
        setGroup(msg);
        this.coordinator = msg.coordinator;
        assignServer();
    }
    // the current implementation sends an update message to a random replica
    // issue: the list of participants needs to be kept up to date wrt replicas that have crashed
    public void onSendUpdate(WriteDataMsg update){
        if(server != null){
            server.tell(new WriteDataMsg(update.value, getSelf()),getSelf());
        }
    }

    @Override
    public void setGroup(JoinGroupMsg sm) {
        for (ActorRef b : sm.group) {
          if (!b.equals(getSelf())) {
            // copying all participant refs except for self
            this.participants.add(b);
          }
        }
        print("starting with " + sm.group.size() + " peer(s)");
      }

    @Override
    public void onRecovery(Recovery msg) {
        // client doesn't crash
    }

    public static class RequestRead{}

    public void assignServer(){
        Iterator<ActorRef> iterator = participants.iterator();
        if (iterator.hasNext()) {
            int randomIndex = (int) (Math.random() * participants.size());
            for (int i = 0; i < randomIndex; i++) {
                iterator.next();
            }
            server = iterator.next();
        }
    }

    public void onTimeout(Timeout msg) {
        // client doesn't crash
        participants.remove(server);
        assignServer();
    }

    public void onRequestRead(RequestRead msg){
        if(server != null){
            server.tell(new ReadDataMsg(getSelf()),getSelf());
            readTimeout = setTimeout(DECISION_TIMEOUT, new Timeout());
        }
    }

    public void onReadResponse(DataMsg res) {
        System.out.println("Client received:"+res.value);
        readTimeout.cancel();
    }
        @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(JoinGroupMsg.class,this::onStartMessage)
                .match(WriteDataMsg.class,this::onSendUpdate)
                .match(DataMsg.class, this::onReadResponse)
                .match(RequestRead.class, this::onRequestRead)
                .match(Timeout.class, this::onTimeout)
                .build();
    }

}
