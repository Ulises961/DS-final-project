/*
 * This Java source file was generated by the Gradle 'init' task.
 */
package org.total_order_broadcast;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;

import org.slf4j.LoggerFactory;

import akka.actor.ActorRef;
import akka.actor.Cancellable;
import akka.actor.Props;

public class Client extends Node {

    private ActorRef server = null;
    private HashSet<ActorRef> participants;
    public static int nextReplica = 0;
    public static int clientNumber = 1;
    private Cancellable readTimeout;
    private Cancellable serverLivenessTimeout;
    private LinkedList<UpdateRequest> updates = new LinkedList<>();
    private boolean checkingServer = false;

    public Client(){
        super(clientNumber++);
        this.participants = new HashSet<>();
        this.logger = LoggerFactory.getLogger(Client.class);

        contextMap = new HashMap<>();
        contextMap.put("replicaId", String.valueOf(id));
    }

    static public Props props() {
        return Props.create(Client.class, Client::new);
    }

    public static class UpdateRequest extends WriteDataMsg {
        public UpdateRequest(Integer value, ActorRef sender, boolean shouldCrash){
            super(value, sender, shouldCrash);
        }
    }

    public void onStartMessage(JoinGroupMsg msg) {
        setGroup(msg);
        this.coordinator = msg.coordinator;
        assignServer();
    }

    public void onSendUpdate(WriteDataMsg update){
        // First check server is alive, 
        // on ping response send update, otherwise choose another server and retry
        updates.add(new UpdateRequest(update.value,update.sender, update.shouldCrash));
        pingServer();
    }

    @Override
    public void setGroup(JoinGroupMsg sm) {
        for (ActorRef b : sm.group) {
          if (!b.equals(getSelf())) {
            // copying all participant refs except for self
            this.participants.add(b);
          }
        }
        logWithMDC( "starting with " + sm.group.size() + " peer(s)");
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
        readTimeout.cancel();
    }

    public void onPong(ICMPResponse msg){
        checkingServer = false;
        serverLivenessTimeout.cancel();
        if(server != null){
            UpdateRequest update = updates.poll();
            if(update != null) {
                server.tell(update,getSelf());
            }
            if(!updates.isEmpty()){
                pingServer();
            }
        }
    }

    public void ICMPTimeout(ICMPTimeout msg){
        checkingServer = false;
        participants.remove(server);
        assignServer();
        pingServer();
    }

    private void pingServer(){
        server.tell(new ICMPRequest(),getSelf());
        if(!checkingServer){
            serverLivenessTimeout = setTimeout(DECISION_TIMEOUT, new ICMPTimeout());
            checkingServer = true;
        }
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
            .match(JoinGroupMsg.class,this::onStartMessage)
            .match(WriteDataMsg.class,this::onSendUpdate)
            .match(DataMsg.class, this::onReadResponse)
            .match(RequestRead.class, this::onRequestRead)
            .match(Timeout.class, this::onTimeout)
            .match(ICMPResponse.class, this::onPong)
            .match(ICMPTimeout.class, this::ICMPTimeout)
            .build();
    }

}
