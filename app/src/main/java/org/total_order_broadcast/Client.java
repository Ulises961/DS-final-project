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

/**
 * Represents a client node in the distributed system.
 * Manages interactions with a set of participants, including sending updates,
 * reading data, and handling server assignments.
 */
public class Client extends Node {

    private ActorRef server = null;
    private HashSet<ActorRef> participants;
    public static int nextReplica = 0;
    private static int clientNumber = 1;
    private Cancellable readTimeout;
    private Cancellable serverLivenessTimeout;
    private LinkedList<UpdateRequest> updates = new LinkedList<>();
    private boolean checkingServer = false;

    /**
     * Constructs a new client instance with an incremented client number.
     */
    public Client(){
        super(clientNumber++);
        this.participants = new HashSet<>();
        this.logger = LoggerFactory.getLogger(Client.class);

        contextMap = new HashMap<>();
        contextMap.put("replicaId", String.valueOf(id));
    }

    /**
     * Constructs a new client instance with a specific client number.
     *
     * @param clientNumber The client number to assign.
     */
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

    /**
     * Inner class representing an update request message for the client.
     */
    public static class UpdateRequest extends WriteDataMsg {
        public UpdateRequest(Integer value, ActorRef sender){
            super(value, sender);
        }

        public String toString() {
            return "UpdateRequest(" + value + ")";
        }
    }

    /**
     * Inner class representing a request to read data from the server.
     */
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

    /**
     * Initializes the client actor's behavior for supervision messages.
     *
     * @return Receive object defining the supervision message handling behavior.
     */
    public Receive supervise() {
        return receiveBuilder()
            .match(SetCoordinator.class, this::onSetCoordinator)
            .match(CrashCoord.class, this::onCrashCoord)
            .match(ReadHistory.class, this::onReadHistory)
            .build();
    }

    /**
     * Sets the group of participants and initializes the client's interactions.
     *
     * @param sm The {@code JoinGroupMsg} containing the group and coordinator information.
     */
    @Override
    public void setGroup(JoinGroupMsg sm) {
        for (ActorRef b : sm.group) {
          if (!b.equals(getSelf())) {
            // copying all participant refs except for self
            this.participants.add(b);
          }
        }
        log("Starting with " + sm.group.size() + " peer(s)", Cluster.LogLevel.DEBUG);
      }

    @Override
    public void onRecovery(Recovery msg) {
        // client doesn't crash
    }

    /**
     * Handles the start message for the client, setting the group and assigning a server.
     *
     * @param msg The {@code JoinGroupMsg} containing group and coordinator information.
     */
    public void onStartMessage(JoinGroupMsg msg) {
        setGroup(msg);
        assignServer();
    }

    /**
     * Handles sending an update message to the server.
     *
     * @param update The {@code WriteDataMsg} containing the update information.
     */
    public void onSendUpdate(WriteDataMsg update){
        // First check server is alive, 
        // on ping response send update, otherwise choose another server and retry
        updates.add(new UpdateRequest(update.value, update.sender));
        pingServer();
    }

    /**
     * Handles a timeout message, indicating server unresponsiveness.
     *
     * @param msg The Timeout message.
     */
    public void onTimeout(Timeout msg) {
        log("Server timeout, changing server " + server.path().name(), Cluster.LogLevel.DEBUG);
        participants.remove(server);
        assignServer();
        onRequestRead(new RequestRead());
    }

    /**
     * Handles a request to read data from the server.
     *
     * @param msg The RequestRead message.
     */
    public void onRequestRead(RequestRead msg){
        if(server != null){
            log("read req to " + server.path().name(), Cluster.LogLevel.INFO);
            server.tell(new ReadDataMsg(getSelf()),getSelf());
            readTimeout = setTimeout(READ_TIMEOUT, new Timeout());
        } 
    }


    public void onReadResponse(DataMsg res) {
        log("read done " + res.value, Cluster.LogLevel.INFO);
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
        log("Coordinator set to " + coordinator.path().name(), Cluster.LogLevel.DEBUG);
    }

    /**
     * Handles supervision message to switch to supervision behavior.
     *
     * @param msg The {@code Supervise} message.
     */
    public void onSupervise(Supervise msg){
        getContext().become(supervise());
    }

    /**
     * Handles crashing the coordinator.
     *
     * @param msg The {@code CrashCoord} message.
     */
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

    /**
     * Assigns a server from the list of participants.
     */
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
