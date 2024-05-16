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
        .build();
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
            // TODO impolement getCoorinator()
            getCoordinator().tell(new SendUpdate(),getSelf());
        }
    }
    public void onStartMessage(StartMessage msg) {
      setGroup(msg);
    }

    public void onVoteRequest(VoteRequest msg) {
      this.coordinator = getSender();
      if (id==2) {crash(5000); return;}    // simulate a crash
      //if (id==2) delay(4000);              // simulate a delay
      if (predefinedVotes[this.id] == Vote.NO) {
        fixDecision(Decision.ABORT);
      }
      print("sending vote " + predefinedVotes[this.id]);
      this.coordinator.tell(new VoteResponse(predefinedVotes[this.id]), getSelf());
      setTimeout(DECISION_TIMEOUT);
    }

    public void onTimeout(Timeout msg) {
        if (!hasDecided()) {
            if (predefinedVotes[this.id] == Vote.YES) {
                print("Timeout. I voted yes. Need to ask around");
                multicast(new DecisionRequest());
                // ask also the coordinator
                coordinator.tell(new DecisionRequest(), getSelf());
                setTimeout(DECISION_TIMEOUT);
            } else {
                //Do nothing as decision is aborted
                print("Timeout. I voted No. I can safely ABORT.");
            }
        }

    }
  }
