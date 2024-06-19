package org.total_order_broadcast;

import akka.actor.*;

import java.io.Serializable;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import org.total_order_broadcast.Client.RequestRead;

public class Replica extends Node {

  HashSet<ActorRef> receivedAcks;
  EpochSeqNum acceptedEpochSeqNum;
  EpochSeqNum proposedEpochSeqNum;
  private boolean expectingAcks = false;
  private boolean nextHopTimedOut = false;
  private int nextHop = -1;

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
        // TODO if we want this, please change what's being sent according to the new implementation of EpochSeqNum
            //.match(DecisionRequest.class, this::onDecisionRequest)
        .match(Heartbeat.class, this::onHeartbeat)
        .match(HeartbeatTimeout.class, this::onHeartbeatTimeout)
        .match(CoordinatorElection.class, this::onCoordinationElection)
        .match(ICMPRequest.class, this::onPing)
            .match(ElectionTimeout.class, this::onElectionTimeout)
            .match(ElectionAck.class, this::onElectionAck)
            .match(ElectionMessage.class, this::onElectionMessageReceipt)
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

      // Assume the heartbeat is received
      renewHeartbeatTimeout();
  }

  public void onTimeout(UpdateTimeOut msg) {
    if (!hasDecided(msg.epochSeqNum)) {
        multicast(new DecisionRequest(msg.epochSeqNum));
        setTimeout(DECISION_TIMEOUT, new UpdateTimeOut(msg.epochSeqNum));
    }
  }

  public void onWriteOk(WriteOk msg) {
    // store the decision
    // fixDecision(msg.value, msg.epochSeqNum);
    fixDecision()
    // Assume the heartbeat is received
    renewHeartbeatTimeout();
  }

  public void onReadMessage(ReadDataMsg msg) { /* Value read from Client */
    msg.sender.tell(new DataMsg(getValue()), getSelf());
  }

  public void onUpdateMessage(WriteDataMsg msg) { /* Value update from Client */

    if (isCoordinator()) {
      // this node is the coordinator so it can send the update to all replicas
      expectingAcks = true;
      // TODO: fix this accoridng to the new imlpementation
      //EpochSeqNum esn = new EpochSeqNum(this.epochSeqNumPair.currentEpoch, this.epochSeqNumPair.seqNum);
      requestUpdate(msg.value, this.epochSeqNumPair.incrementSeqNum(msg.value));
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
      multicast(new Heartbeat());
      
      if(this.heartbeatTimeout != null) {
        this.heartbeatTimeout.cancel();
      }
      heartbeatTimeout = setTimeout(this.HEARTBEAT_INTERVAL, new Heartbeat());

    } else {
      renewHeartbeatTimeout();
    }
  }

  public void onHeartbeatTimeout(HeartbeatTimeout msg) {
    System.out.println("Coordinator is not responding. Starting election." + self().path().name());
    nextHop = sendElectionMsg(new ElectionMessage(updateHistory,getSelf(),this.id),nextHop);
    // TODO: set timer, if it expires, remove "nextHop" and try again;
    // because of the assumption that there will ALWAYS be a quorum we can safely say that
    // next hop will NEVER be this node thanks to the assumption made above ^^^^
    electionTimeout = setTimeout(HEARTBEAT_TIMEOUT_DURATION, new ElectionTimeout(nextHop));
  }

  public void onElectionAck(ElectionAck ack) {
    electionTimeout.cancel();
    // eventually this will be set to false (assumption that there is always at least a quorum
    nextHopTimedOut = false;
  }

  // if nextHop has crashed, we try with the node after that, until someone sends back an Ack
  public void onElectionTimeout(ElectionTimeout msg) {
    nextHopTimedOut = true;
    // because nextHop timedout we try with the one after that: nextHop+1;
    nextHop = sendElectionMsg(new ElectionMessage(updateHistory,getSelf(),this.id),msg.next);
    // set timeout with nextHop+1
    electionTimeout = setTimeout(HEARTBEAT_TIMEOUT_DURATION, new ElectionTimeout(nextHop));
  }

  public void onCoordinationElection(CoordinatorElection msg) {
    System.out.println("Election message received. Starting election.");
  }

  public void onPing(ICMPRequest msg) {
    System.out.println("Received ping from " + getSender().path().name());
    getSender().tell(new ICMPResponse(), getSelf());
  }

  private boolean isCoordinator() {
    if (this.coordinator == null) {
      System.out.println("Coordinator is not set");
      return false;
    }
    return coordinator.equals(getSelf());
  }

  private void renewHeartbeatTimeout(){
    if(this.heartbeatTimeout != null) {
      this.heartbeatTimeout.cancel();
    }
    heartbeatTimeout = setTimeout(HEARTBEAT_TIMEOUT_DURATION, new HeartbeatTimeout());
  }

  // election message, this is what each replica receives/sends
  public static class ElectionMessage implements Serializable {
    public ActorRef proposedCoordinator;
    Map<EpochSeqNum, Integer> updateHistory;
    int proposedCoordinatorID;

    public ElectionMessage(Map<EpochSeqNum, Integer> updateHistory, ActorRef coord, int proposedCoordinatorID) {
      this.updateHistory = updateHistory;
      this.proposedCoordinatorID = proposedCoordinatorID;
      this.proposedCoordinator = coord;
    }

  }

  /*
  Logic: sort list of participants, get replica whose id > this.id, then try to send, if timeout update nextHop
   */
  private int sendElectionMsg(ElectionMessage electionMessage, int next){
    if (next == -1) {
      Collections.sort(participants);
      int pos = participants.indexOf(this);
      participants.get((pos + 1) % participants.size()).tell(electionMessage, getSelf());
      return (pos + 1) % participants.size();
    }else if (nextHopTimedOut){
      participants.get((next+1) % participants.size()).tell(electionMessage, getSelf());
      return (next + 1) % participants.size();
    }else{
      participants.get((next) % participants.size()).tell(electionMessage, getSelf());
      return (next) % participants.size();
    }
  }

  public void updateCoordinator(ActorRef coordinator){
    this.coordinator = coordinator;
  }

  // on election message receipt, check if up to date
  public void onElectionMessageReceipt(ElectionMessage electionMessage){
    // send ack to whoever sent the message
    getSender().tell(new ElectionAck(),getSelf());
    // view message content
    ActorRef proposedCoord = electionMessage.proposedCoordinator;
    int proposedCoordinatorID = electionMessage.proposedCoordinatorID;
    // this would be much easier if updateHistory were a Set rather than a Map

    List<EpochSeqNum> epochSeqNumList = (List<EpochSeqNum>) electionMessage.updateHistory.keySet();

    Map<EpochSeqNum, Integer> update = electionMessage.updateHistory;
    // sorting epochs-seqNums in decreasing order;
    epochSeqNumList.sort((o1,o2) -> o1.currentEpoch < o2.currentEpoch ? 1 : -1);
    if (epochSeqNumList.get(0).getCurrentEpoch() > this.epochSeqNumPair.getCurrentEpoch()){
      // if we're here we missed out 1+ epochs, we must update our history
      int diff = Math.abs(epochSeqNumList.get(0).getCurrentEpoch() - this.epochSeqNumPair.getCurrentEpoch());
      // since epoch updates are sequential we can iterate over the difference diff.
      int pos = 0;
      while(pos < diff){
        // there's probably a better way to do this but I can't think of it rn :)
        // the idea is that we get the epochSeqNum from the List, then we obtain the value from the hashmap
        // and add those to our updateHistory
        EpochSeqNum key = epochSeqNumList.get(pos);
        Integer value = electionMessage.updateHistory.get(key);
        this.updateHistory.put(key,value);
        pos++;
      }
      // update self, not the message.
      updateCoordinator(electionMessage.proposedCoordinator);
    } else if (epochSeqNumList.get(0).getCurrentEpoch() < this.epochSeqNumPair.getCurrentEpoch()) {
      // we have the latest version so far, we must update the message and forward it.
      proposedCoord = getSelf();
      proposedCoordinatorID = this.id;
      update = this.updateHistory;
    }else{
      // check for missed seqNum updates within epochs

    }
    sendElectionMsg(new ElectionMessage(update,proposedCoord, proposedCoordinatorID),nextHop);
  }

}
