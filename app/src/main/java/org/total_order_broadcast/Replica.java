package org.total_order_broadcast;

import akka.actor.*;

import java.io.Serializable;
import java.util.HashSet;

public class Replica extends Node {
    ActorRef coordinator;
    HashSet<ActorRef> receivedAcks;
    EpochSeqNum acceptedEpochSeqNum;
    EpochSeqNum proposedEpochSeqNum;
    private boolean expectingAcks = false;

    public Replica(int id, boolean isCoordinator) {
        super(id, isCoordinator);
        this.receivedAcks = new HashSet<>();
    }

    @Override
    protected void onRecovery(Recovery msg) {
        // this is pointless as nodes don't recover after crashing (see slides)
    }

    static public Props props(int id,  boolean isCoordinator) {
      return Props.create(Replica.class, () -> new Replica(id, isCoordinator));
    }
    public static class UpdateMsg implements Serializable{
        public final EpochSeqNum epochSeqNum;
        public UpdateMsg(EpochSeqNum esn){
            this.epochSeqNum = esn;
        }
    }
    public static class Ack implements Serializable{}
    public static class WriteOk implements Serializable{}
    @Override
    public Receive createReceive() {
      return receiveBuilder()
        .match(StartMessage.class, this::onStartMessage)
        .match(Timeout.class, this::onTimeout)
        .match(Recovery.class, this::onRecovery)
        .match(UpdateMsg.class, this::onUpdate)
        .match(Ack.class, this::onAckReceived)
        .match(SendUpdate.class,this::onSendUpdate)
        .match(WriteOk.class, this::onWriteOk)
              .match(Client.RequestRead.class, this::onRequestRead)
        .build();
    }

    public void onRequestRead(Client.RequestRead msg) {
        getSender().tell(msg.setEpochSeqNum(this.epochSeqNumPair), getSelf());
    }

    public void onUpdate(UpdateMsg update){
        this.proposedEpochSeqNum = update.epochSeqNum;
        // send ack
        getSender().tell(new Ack(), getSelf());
    }
    public void onWriteOk(WriteOk okay){
        this.acceptedEpochSeqNum = this.proposedEpochSeqNum;
    }
    public void onAckReceived(Ack a){
        if (expectingAcks) {
            ActorRef sender = getSender();
            receivedAcks.add(sender);
            if (receivedAcks.size() >= quorum) { // enough acks received, now send WRITEOK message
                multicast(new WriteOk());
                expectingAcks = false; // update phase completed, no more acks expected
            }
        }
    }
    // either starts the update or forwards message to the coordinator
    public void onSendUpdate(SendUpdate update){
        if (this.isCoordinator){
            // this node is the coordinator so it can send the update to all replicas
            expectingAcks = true;
            multicast(new UpdateMsg(epochSeqNumPair.increaseSeqNum()));
            // TODO start timer, if the timer expires rollback seqNumber i.e. epochSeqNumPair.rollbackSeqNum()
        }else{
            // forward request to the coordinator
            // TODO implement getCoordinator()
            getCoordinator().tell(new SendUpdate(),getSelf());
        }
    }
    public void onStartMessage(StartMessage msg) {
      setGroup(msg);
    }

    public void onTimeout(Timeout msg) {

    }
  }
