package org.total_order_broadcast;

import akka.actor.*;

import java.io.Serializable;
import java.util.HashSet;

import org.total_order_broadcast.Client.RequestRead;

public class Replica extends Node {

  HashSet<ActorRef> receivedAcks;
  EpochSeqNum acceptedEpochSeqNum;
  EpochSeqNum proposedEpochSeqNum;
  private boolean expectingAcks = false;

  public Replica(int id) {
    super(id);
    this.receivedAcks = new HashSet<>();
    System.out.println("Replica " + id + " created");
  }

  @Override
  protected void onRecovery(Recovery msg) {
    return; // An Actor needs to implement an onRecovery method./
  }

  static public Props props(int id) {
    return Props.create(Replica.class, () -> new Replica(id));
  }

  public static class UpdateMsg implements Serializable {
    public final EpochSeqNum epochSeqNum;

    public UpdateMsg(EpochSeqNum esn) {
      this.epochSeqNum = esn;
    }
  }

  @Override
  public Receive createReceive() {
    return receiveBuilder()
        .match(JoinGroupMsg.class, this::onStartMessage)
        .match(UpdateTimeOut.class, this::onTimeout)
        .match(Recovery.class, this::onRecovery)
        .match(WriteDataMsg.class, this::onUpdateMessage)
        .match(ReadDataMsg.class, this::onReadMessage)
        .match(UpdateRequest.class, this::onUpdateRequest)
        .match(UpdateAck.class, this::onUpdateAck)
        .match(WriteOk.class, this::onWriteOk)
        .match(RequestRead.class, this::onRequestRead)
        .match(DecisionRequest.class, this::onDecisionRequest)
        .match(Heartbeat.class, this::onHeartbeat)
        .match(HeartbeatTimeout.class, this::onHeartbeatTimeout)
        .match(CoordinatorElection.class, this::onCoordinationElection)
        .build();
  }

  public void onRequestRead(RequestRead msg) {
    getSender().tell(currentValue, getSelf());
  }

  public void onUpdateAck(UpdateAck ack) {
    if (expectingAcks) {
      ActorRef sender = getSender();
      receivedAcks.add(sender);
      if (receivedAcks.size() >= quorum) { // enough acks received, now send WRITEOK message
        print("Quorum reached, sending Ok for value " + ack.value);
        multicast(new WriteOk(ack.value, ack.epochSeqNum));
        expectingAcks = false; // update phase completed, no more acks expected
      }
    }
  }

  public void onStartMessage(JoinGroupMsg msg) {
    setGroup(msg);
    this.coordinator = msg.coordinator;
    if (isCoordinator()) {
      multicast(new Heartbeat());
    }
  }

  public void onUpdateRequest(UpdateRequest msg) {
    // if (id==2) {crash(5000); return;} // simulate a crash
    // if (id==2) delay(4000); // simulate a delay
    
    // Updates must be monotonically increasing within the latest epoch 
      expectingAcks = true;
      coordinator.tell(new UpdateAck(msg.value, msg.epochSeqNum), getSelf());
      setTimeout(DECISION_TIMEOUT, new UpdateTimeOut(msg.epochSeqNum));
  }

  public void onTimeout(UpdateTimeOut msg) {
    if (!hasDecided(msg.epochSeqNum)) {
        multicast(new DecisionRequest(msg.epochSeqNum));
        setTimeout(DECISION_TIMEOUT, new UpdateTimeOut(msg.epochSeqNum));
    }
  }

  public void onWriteOk(WriteOk msg) {
    // store the decision
    fixDecision(msg.value, msg.epochSeqNum);
  }

  public void onReadMessage(ReadDataMsg msg) { /* Value read from Client */
    msg.sender.tell(new DataMsg(getValue()), getSelf());
  }

  public void onUpdateMessage(WriteDataMsg msg) { /* Value update from Client */

    if (isCoordinator()) {
      // this node is the coordinator so it can send the update to all replicas
      expectingAcks = true;
      this.epochSeqNumPair.increaseSeqNum();
      EpochSeqNum esn = new EpochSeqNum(this.epochSeqNumPair.epoch, this.epochSeqNumPair.seqNum);
      requestUpdate(msg.value, esn);
    } else {
      // forward request to the coordinator
      coordinator.tell(msg, getSelf());
    }
  }

  private void requestUpdate(Integer value, EpochSeqNum seqNum) {
    multicast(new UpdateRequest(value, seqNum));
    setTimeout(DECISION_TIMEOUT, new UpdateTimeOut(seqNum));
    System.out.println("Requesting update from replicas. Value proposed: " + value + " SeqNum: " + seqNum);
  }

  public void onHeartbeat(Heartbeat msg) {
    if (isCoordinator()) {
      setTimeout(this.HEARTBEAT_INTERVAL, new Heartbeat());
    } else {
      setTimeout(this.HEARTBEAT_TIMEOUT, new HeartbeatTimeout());
    }
  }

  public void onHeartbeatTimeout(HeartbeatTimeout msg) {
    System.out.println("Coordinator is not responding. Starting election.");
    multicast(new CoordinatorElection(this.id));
  }

  public void onCoordinationElection(CoordinatorElection msg) {
    System.out.println("Election message received. Starting election.");
  }

  private boolean isCoordinator() {
    if (this.coordinator == null) {
      System.out.println("Coordinator is not set");
      return false;
    }
    return coordinator.equals(getSelf());
  }
}
