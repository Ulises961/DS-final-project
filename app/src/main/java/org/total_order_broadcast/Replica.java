package org.total_order_broadcast;

import akka.actor.*;

import java.io.Serializable;
import java.util.HashSet;

public class Replica extends Node {

  HashSet<ActorRef> receivedAcks;
  HashSet<ActorRef> receivedVotes;
  EpochSeqNum acceptedEpochSeqNum;
  EpochSeqNum proposedEpochSeqNum;
  private boolean expectingAcks = false;
  private boolean expectingVotes = false;

  public Replica(int id) {
    super(id);
    this.receivedAcks = new HashSet<>();
    this.receivedVotes = new HashSet<>();
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

  public static class Ack implements Serializable {
  }

  public static class WriteOk implements Serializable {
  }

  @Override
  public Receive createReceive() {
    return receiveBuilder()
        .match(JoinGroupMsg.class, this::onStartMessage)
        .match(Timeout.class, this::onTimeout)
        .match(Recovery.class, this::onRecovery)
        .match(WriteDataMsg.class, this::onUpdateMessage)
        .match(ReadDataMsg.class, this::onReadMessage)
        .match(ProposeValueMsg.class, this::onProposeMessage)
        .match(VoteRequest.class, this::onVoteRequest)
        .match(VoteResponse.class, this::onVoteResponse)
        .match(Ack.class, this::onAckReceived)
        .match(WriteOk.class, this::onWriteOk)
        .match(Client.RequestRead.class, this::onRequestRead)
        .match(DecisionRequest.class, this::onDecisionRequest)
        .match(DecisionResponse.class, this::onDecisionResponse)
        .build();
  }

  public void onRequestRead(Client.RequestRead msg) {
    getSender().tell(msg.setEpochSeqNum(this.epochSeqNumPair), getSelf());
  }

  public void onWriteOk(WriteOk okay) {
    this.acceptedEpochSeqNum = this.proposedEpochSeqNum;
  }

  public void onAckReceived(Ack a) {
    if (expectingAcks) {
      ActorRef sender = getSender();
      receivedAcks.add(sender);
      if (receivedAcks.size() >= quorum) { // enough acks received, now send WRITEOK message
        multicast(new WriteOk());
        expectingAcks = false; // update phase completed, no more acks expected
      }
    }
  }

  public void onVoteResponse(VoteResponse response) {
    print("Expecting votes? " + expectingVotes + " Vote: " + response.vote + " Value: " + response.value);
    if (expectingVotes) {
      ActorRef sender = getSender();
      receivedVotes.add(sender);
      if(response.vote == Vote.NO){
        fixDecision(Decision.ABORT, null);
        multicast(new DecisionResponse(Decision.ABORT, null));
        expectingVotes = false; // update phase completed, no more acks expected

      }
      if (receivedVotes.size() >= quorum) { // enough acks received, now send WRITEOK message
        multicast(new DecisionResponse(Decision.COMMIT, proposedValue));
        expectingVotes = false; // update phase completed, no more acks expected
      }
    }
  }

  public void onStartMessage(JoinGroupMsg msg) {
    setGroup(msg);
    this.coordinator = msg.coordinator;
  }

  public void onVoteRequest(VoteRequest msg) {
    // if (id==2) {crash(5000); return;} // simulate a crash
    // if (id==2) delay(4000); // simulate a delay
    
    print("sending vote " + predefinedVotes[this.id]);
    if (predefinedVotes[this.id] == Vote.NO) {
      fixDecision(Decision.ABORT, getValue());
    }
    expectingVotes = true;
    coordinator.tell(new VoteResponse(predefinedVotes[this.id], msg.value, msg.epochSeqNumPair), getSelf());
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
        // Do nothing as decision is aborted
        print("Timeout. I voted No. I can safely ABORT.");
      }
    }
  }

  public void onDecisionResponse(DecisionResponse msg) { /* Decision Response */
    // store the decision
    print("Decision received: " + msg.decision + " Value: " + msg.value);
    if(msg.decision == Decision.COMMIT){
      fixDecision(msg.decision, msg.value);
    } else {
      epochSeqNumPair.rollbackSeqNum();
    }
  }

  public void onReadMessage(ReadDataMsg msg) { /* Value read from Client */
    getSender().tell(new DataMsg(getValue(), getSelf()), getSelf());
  }

  public void onUpdateMessage(WriteDataMsg msg) { /* Value update from Client */
    this.proposedValue = msg.value;

    if (isCoordinator()) {
      // this node is the coordinator so it can send the update to all replicas
      expectingAcks = true;
      requestVote(msg.value, this.epochSeqNumPair.increaseSeqNum());
    } else {
      // forward request to the coordinator
      coordinator.tell(new ProposeValueMsg(msg.value), getSelf());
      System.out.println(
          "Forwarding proposal to coordinator. Value proposed: " + msg.value + " SeqNum: " + this.epochSeqNumPair);
    }
  }

  public void onProposeMessage(ProposeValueMsg msg) { /* Value proposal from Replica */
    if (isCoordinator()) {
      requestVote(msg.value, this.epochSeqNumPair.increaseSeqNum());
    }
  }

  private void requestVote(Integer value, EpochSeqNum seqNum) {
    proposedValue = value;
    multicast(new VoteRequest(value, seqNum));
    setTimeout(1000);
    System.out.println("Requesting vote from replicas. Value proposed: " + value + " SeqNum: " + seqNum);
  }

  protected boolean isCoordinator() {
    if (this.coordinator == null) {
      System.out.println("Coordinator is not set");
      return false;
    }
    return coordinator.equals(getSelf());
  }
}
