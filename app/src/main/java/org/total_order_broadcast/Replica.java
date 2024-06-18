package org.total_order_broadcast;

import akka.actor.*;
import scala.util.Random;

import java.io.Serializable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.total_order_broadcast.Client.RequestRead;

public class Replica extends Node {

  HashSet<ActorRef> receivedAcks;
  EpochSeqNum acceptedEpochSeqNum;
  EpochSeqNum proposedEpochSeqNum;
  private boolean expectingAcks = false;

  protected int quorum;

  protected Integer currentValue;

  // participants (initial group, current and proposed views)
  protected final Set<ActorRef> group;

  protected final Set<ActorRef> currentView;

  // each view has is assocaited w/ an Epoch
  protected final Map<EpochSeqNum, Set<ActorRef>> proposedView;

  // last sequence number for each node message (to avoid delivering duplicates)
  protected final Map<ActorRef, Integer> membersSeqno;

  // history of updates
  protected final Map<EpochSeqNum, Integer> updateHistory;

  // deferred messages (of a future view)
  protected final Set<WriteDataMsg> deferredMsgSet;

  // group view flushes
  protected final Map<Integer, Set<ActorRef>> flushes;

  // cancellation of the heartbeat timeout
  protected Cancellable heartbeatTimeout;

  // to include delays in the messages
  private Random rnd = new Random();
  // dedicated class to keep track of epoch and seqNum pairs :)
  protected EpochSeqNum epochSeqNumPair;


  public Replica(int id) {
    super(id);
    this.receivedAcks = new HashSet<>();
    this.quorum = (N_PARTICIPANTS / 2) + 1;
    this.epochSeqNumPair = new EpochSeqNum(0, 0);
    this.group = new HashSet<>();
    this.currentView = new HashSet<>();
    this.proposedView = new HashMap<>();
    this.membersSeqno = new HashMap<>();
    this.updateHistory = new HashMap<>();
    this.deferredMsgSet = new HashSet<>();
    this.flushes = new HashMap<>();

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
        .match(ICMPRequest.class, this::onPing)
        .build();
  }

  private void updateQuorum() {
    this.quorum = currentView.size() / 2 + 1;
  }

  public void setValue(int value) {
    this.currentValue = value;
  }

  public int getValue() {
    return currentValue != null ? currentValue : 0;
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
    fixDecision(msg.value, msg.epochSeqNum);

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
      this.epochSeqNumPair.increaseSeqNum();
      EpochSeqNum esn = new EpochSeqNum(this.epochSeqNumPair.epoch, this.epochSeqNumPair.seqNum);
      requestUpdate(msg.value, esn);
    } else {
      // forward request to the coordinator
      coordinator.tell(msg, getSelf());
    }
  }

  public void onDecisionRequest(DecisionRequest msg) { /* Decision Request */
    Integer historicValue = getHistoricValue(msg.epochSeqNum);
    if (historicValue != null) {
      getSender().tell(new WriteOk(historicValue, msg.epochSeqNum), getSelf());
      String message = "received decision request from " +
          getSender();
      print(message);
    }
  }

  private Integer getHistoricValue(EpochSeqNum esn) {
    for (Map.Entry<EpochSeqNum, Integer> entry : updateHistory.entrySet()) {
      EpochSeqNum key = entry.getKey();
      if (key.epoch == esn.epoch && key.seqNum == esn.seqNum) {
        return entry.getValue();
      }
    }
    return null;
  }

  public void multicast(Serializable m) {
    for (ActorRef p : participants) {
      delay(rnd.nextInt(RANDOM_DELAY));
      p.tell(m, getSelf());

    }
  }

  // a multicast implementation that crashes after sending the first message
  public void multicastAndCrash(Serializable m) {
    for (ActorRef p : participants) {
      delay(rnd.nextInt(RANDOM_DELAY));
      p.tell(m, getSelf());
      crash();
      return;
    }
  }


  // fix the final decision of the current node
  public void fixDecision(Integer v, EpochSeqNum epochSeqNum) {
    if (!hasDecided(epochSeqNum)) {
      currentValue = v;
      updateHistory.put(epochSeqNum, v);

      print("Fixing value " + currentValue);
      print("Update History " + updateHistory.toString());
    }
  }

  boolean hasDecided(EpochSeqNum epochSeqNum) {
    return this.updateHistory.get(epochSeqNum) != null;
  } // has the node decided?

  private void requestUpdate(Integer value, EpochSeqNum seqNum) {
    multicast(new UpdateRequest(value, seqNum));
    setTimeout(DECISION_TIMEOUT, new UpdateTimeOut(seqNum));
    System.out.println("Requesting update from replicas. Value proposed: " + value + " SeqNum: " + seqNum);
  }

  public void onHeartbeat(Heartbeat msg) {
    if (isCoordinator()) {
      multicast(new Heartbeat());

      if (this.heartbeatTimeout != null) {
        this.heartbeatTimeout.cancel();
      }
      heartbeatTimeout = setTimeout(this.HEARTBEAT_INTERVAL, new Heartbeat());

    } else {
      renewHeartbeatTimeout();
    }
  }

  public void onHeartbeatTimeout(HeartbeatTimeout msg) {
    System.out.println("Coordinator is not responding. Starting election." + self().path().name());
    multicast(new CoordinatorElection(this.id));
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

  private void renewHeartbeatTimeout() {
    if (this.heartbeatTimeout != null) {
      this.heartbeatTimeout.cancel();
    }
    heartbeatTimeout = setTimeout(HEARTBEAT_TIMEOUT_DURATION, new HeartbeatTimeout());
  }
}
