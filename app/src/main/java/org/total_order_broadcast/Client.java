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
    private static int clientNumber = 1;
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
    public Client(int clientNumber){
        super(clientNumber);
        this.participants = new HashSet<>();
        this.logger = LoggerFactory.getLogger(Client.class);

        contextMap = new HashMap<>();
        contextMap.put("replicaId", String.valueOf(id));
    }

    static public Props props() {
        return Props.create(Client.class, Client::new);
    }
    
    public static Props props(int clientId) {
        return Props.create(Client.class, () -> new Client(clientId));
    }

    public static class SetCoordinator {}

    public static class Supervise {}

    public static class UpdateRequest extends WriteDataMsg {
        public UpdateRequest(Integer value, ActorRef sender, boolean shouldCrash){
            super(value, sender, shouldCrash);
        }

        public String toString() {
            return "UpdateRequest(" + value + ")";
        }
    }

    public static class RequestRead {}

      @Override
    public Receive createReceive() {
        return receiveBuilder()
            .match(JoinGroupMsg.class,this::onStartMessage)
            .match(WriteDataMsg.class,this::onSendUpdate)
            .match(DataMsg.class, this::onReadResponse)
            .match(RequestRead.class, this::onRequestRead)
            .match(ICMPResponse.class, this::onICMPResponse)
            .match(ICMPTimeout.class, this::ICMPTimeout)
            .match(Timeout.class, this::onTimeout)
            .match(Supervise.class, this::onSupervise)
            .build();
    }
   
    public Receive supervise() {
        return receiveBuilder()
            .match(SetCoordinator.class, this::onSetCoordinator)
            .match(CrashCoord.class, this::onCrashCoord)
            .match(ReadHistory.class, this::onReadHistory)
            .build();
    }

    @Override
    public void setGroup(JoinGroupMsg sm) {
        for (ActorRef b : sm.group) {
          if (!b.equals(getSelf())) {
            // copying all participant refs except for self
            this.participants.add(b);
          }
        }
        log("Starting with " + sm.group.size() + " peer(s)", LogLevel.DEBUG);
      }

    @Override
    public void onRecovery(Recovery msg) {
        // client doesn't crash
    }

    public void onStartMessage(JoinGroupMsg msg) {
        setGroup(msg);
        assignServer();
    }

    public void onSendUpdate(WriteDataMsg update){
        // First check server is alive, 
        // on ping response send update, otherwise choose another server and retry
        updates.add(new UpdateRequest(update.value, update.sender, update.shouldCrash));
        pingServer();
    }

    public void onTimeout(Timeout msg) {
        log("Server timeout, changing server " + server.path().name(), LogLevel.DEBUG);
        participants.remove(server);
        assignServer();
        onRequestRead(new RequestRead());
    }

    public void onRequestRead(RequestRead msg){
        if(server != null){
            log("read req to " + server.path().name(), LogLevel.INFO);
            server.tell(new ReadDataMsg(getSelf()),getSelf());
            readTimeout = setTimeout(READ_TIMEOUT, new Timeout());
        } 
    }

    public void onReadResponse(DataMsg res) {
        log("read done " + res.value, LogLevel.INFO);
        readTimeout.cancel();
    }

    public void onICMPResponse(ICMPResponse msg){
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

    public void onSetCoordinator(SetCoordinator msg){
        coordinator = getSender();
        log("Coordinator set to " + coordinator.path().name(), LogLevel.DEBUG);
    }

    public void onSupervise(Supervise msg){
        getContext().become(supervise());
    }

    public void onCrashCoord(CrashCoord msg){
        if(coordinator != null) {
            coordinator.tell(new CrashMsg(),getSelf());
        }
    }

    public void onReadHistory(ReadHistory msg){
        if(coordinator != null) {
            coordinator.tell(msg,getSelf());
        }
    }

    private void pingServer(){
        server.tell(new ICMPRequest(),getSelf());
        if(!checkingServer){
            serverLivenessTimeout = setTimeout(DECISION_TIMEOUT, new ICMPTimeout());
            checkingServer = true;
        }
    }

    private void assignServer(){
        Iterator<ActorRef> iterator = participants.iterator();
        if (iterator.hasNext()) {
            int randomIndex = (int) (Math.random() * participants.size());
            for (int i = 0; i < randomIndex; i++) {
                iterator.next();
            }
            server = iterator.next();
        }
    }
}
