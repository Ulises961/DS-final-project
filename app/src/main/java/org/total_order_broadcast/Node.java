package org.total_order_broadcast;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.io.Serializable;

import scala.concurrent.duration.Duration;
import scala.util.Random;
import akka.actor.*;

public abstract class Node extends AbstractActor {

  protected int id; // node ID

  protected List<ActorRef> participants; // list of participant nodes

  protected int quorum;

  protected Integer currentValue;

  // dedicated class to keep track of epoch and seqNum pairs :)
  protected EpochSeqNum epochSeqNumPair;

  // whether the node should join through the manager
  protected ActorRef coordinator;

  // participants (initial group, current and proposed views)
  protected final Set<ActorRef> group;

  protected final Set<ActorRef> currentView;

  // each view has is assocaited w/ an Epoch
  protected final Map<EpochSeqNum, Set<ActorRef>> proposedView;

  // last sequence number for each node message (to avoid delivering duplicates)
  protected final Map<ActorRef, Integer> membersSeqno;

  // history of updates
  protected Map<EpochSeqNum, Integer> updateHistory;

  // deferred messages (of a future view)
  protected final Set<WriteDataMsg> deferredMsgSet;

  // group view flushes
  protected final Map<Integer, Set<ActorRef>> flushes;

  // cancellation of the heartbeat timeout
  protected Cancellable heartbeatTimeout;

  protected Cancellable electionTimeout;

  // to include delays in the messages
  private Random rnd = new Random();

  // TODO missed updates message to bring replicas up to date, from the
  // coordinator

  // Hearbeat
  protected final int HEARTBEAT_TIMEOUT_DURATION = 2000;
  protected final int HEARTBEAT_INTERVAL = 1000;

  final static int N_PARTICIPANTS = 3;
  final static int VOTE_TIMEOUT = 500; // timeout for the votes, ms
  final static int DECISION_TIMEOUT = 100; // timeout for the decision, ms
  final static int RANDOM_DELAY = Math.round((VOTE_TIMEOUT + 50) / N_PARTICIPANTS); // timeout for the decision, ms

  public Node(int id) {
    super();
    this.id = id;
    this.epochSeqNumPair = new EpochSeqNum(0, 0);
    this.group = new HashSet<>();
    this.currentView = new HashSet<>();
    this.proposedView = new HashMap<>();
    this.membersSeqno = new HashMap<>();
    this.deferredMsgSet = new HashSet<>();
    this.flushes = new HashMap<>();
    this.quorum = (N_PARTICIPANTS / 2) + 1;
    updateHistory = new HashMap<>();
    updateHistory.put(epochSeqNumPair, 0);
  }

  private void updateQuorum() {
    this.quorum = currentView.size() / 2 + 1;
  }


  // Start message that sends the list of participants to everyone
  public static class StartMessage implements Serializable {
    public final List<ActorRef> group;
    public final ActorRef coordinator;

    public StartMessage(List<ActorRef> group, ActorRef coordinator) {
      this.group = Collections.unmodifiableList(new ArrayList<>(group));
      this.coordinator = coordinator;
    }
  }

  public static class UpdateRequest implements Serializable {
    public final Integer value;
    public final EpochSeqNum epochSeqNum;

    public UpdateRequest(Integer value, EpochSeqNum epochSeqNum) {
      this.value = value;
      this.epochSeqNum = epochSeqNum;
    }
  }

  public static class SyncMessage implements Serializable {
    public Map<EpochSeqNum, Integer> updateHistory;
    public SyncMessage(Map<EpochSeqNum, Integer> updateHistory) {
      this.updateHistory = Collections.unmodifiableMap(new HashMap<EpochSeqNum,Integer>(updateHistory));
    }
  }

  public static class UpdateAck implements Serializable {
    public final Integer value;
    public final EpochSeqNum epochSeqNum;

    public UpdateAck(Integer val, EpochSeqNum epochSeqNum) {
      value = val;
      this.epochSeqNum = epochSeqNum;
    }
  }

  public static class DecisionRequest implements Serializable {
    public final EpochSeqNum epochSeqNum;

    public DecisionRequest(EpochSeqNum epochSeqNum) {
      this.epochSeqNum = epochSeqNum;
    }
  }

  public static class WriteOk implements Serializable {
    public final Integer value;
    public final EpochSeqNum epochSeqNum;

    public WriteOk(Integer v, EpochSeqNum epochSeqNum) {
      value = v;
      this.epochSeqNum = epochSeqNum;
    }
  }

  public static class Timeout implements Serializable {
  }

  public static class UpdateTimeOut extends Timeout {
    public final EpochSeqNum epochSeqNum;

    public UpdateTimeOut(EpochSeqNum esn) {
      epochSeqNum = esn;
    }
  }

  public static class Recovery implements Serializable {
  }

  public static class JoinGroupMsg implements Serializable {
    public final List<ActorRef> group; // an array of group members
    public final ActorRef coordinator; // the coordinator

    public JoinGroupMsg(List<ActorRef> group, ActorRef coordinator) {
      this.group = Collections.unmodifiableList(new ArrayList<>(group));
      this.coordinator = coordinator;
    }
  }

  public static class ReadDataMsg implements Serializable {
    ActorRef sender;

    public ReadDataMsg(ActorRef sender) {
      this.sender = sender;
    }
  }

  public static class DataMsg implements Serializable {
    Integer value;

    public DataMsg(Integer value) {
      this.value = value;
    }
  }

  public static class CoordinatorElection implements Serializable {
    Integer proposedCoordinatorId;

    public CoordinatorElection(Integer proposedCoordinatorId) {
      this.proposedCoordinatorId = proposedCoordinatorId;
    }
  }

  public static class ElectionAck implements Serializable {}

  public static class Heartbeat extends Timeout {}

  public static class HeartbeatTimeout extends Timeout {}

  public static class ElectionTimeout extends Timeout {
    int next;
    public ElectionTimeout(int next) {
      this.next = next;
    }
  }

  public static class WriteDataMsg implements Serializable {
    public final Integer value;
    public final boolean shouldCrash;
    ActorRef sender;

    public WriteDataMsg(Integer value, ActorRef sender, boolean shouldCrash) {
      this.value = value;
      this.sender = sender;
      this.shouldCrash = shouldCrash;
    }

    public WriteDataMsg(Integer value, ActorRef sender) {
      this.value = value;
      this.sender = sender;
      this.shouldCrash = false;
    }
  }

  public static class ViewChangeMsg implements Serializable {
    public final Integer viewId;
    public final Set<ActorRef> proposedView;

    public ViewChangeMsg(int viewId, Set<ActorRef> proposedView) {
      this.viewId = viewId;
      this.proposedView = Collections.unmodifiableSet(new HashSet<>(proposedView));
    }
  }

  public static class CrashMsg implements Serializable {}

  public static class RecoveryMsg implements Serializable {}

  public static class ICMPRequest implements Serializable {}
  public static class ICMPResponse implements Serializable {}

  public static class ICMPTimeout extends Timeout {}
  
  public static class UpdateMsg implements Serializable {
    public final EpochSeqNum epochSeqNum;

    public UpdateMsg(EpochSeqNum esn) {
      this.epochSeqNum = esn;
    }
  }
  public void setValue(int value) {
    this.currentValue = value;
  }

  public int getValue() {
    return currentValue != null ? currentValue : 0;
  }

  // abstract method to be implemented in extending classes
  protected abstract void onRecovery(Recovery msg);

  void setGroup(JoinGroupMsg sm) {
    participants = new ArrayList<>();
    for (ActorRef b : sm.group) {
      this.participants.add(b);
    }
    print("Starting with " + sm.group.size() + " peer(s)");
  }

  // emulate a crash and a recovery in a given time
  void crash() {
    getContext().become(crashed());
    print("CRASH!!!");
  }

  public Receive crashed() {
    return receiveBuilder()
            .matchAny(msg -> {})
            .build();
  }

  // emulate a delay of d milliseconds
  void delay(int d) {
    try {
      Thread.sleep(d);
    } catch (Exception ignored) {
    }
  }

  void multicast(Serializable m) {
    for (ActorRef p : participants) {
      try {
        Thread.sleep(rnd.nextInt(RANDOM_DELAY));
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
      p.tell(m, getSelf());

    }
  }

  // a multicast implementation that crashes after sending the first message
  void multicastAndCrash(Serializable m) {
    for (ActorRef p : participants) {
      try {
        Thread.sleep(rnd.nextInt(RANDOM_DELAY));
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
      p.tell(m, getSelf());
      crash();
      return;
    }
  }

  // schedule a Timeout message in specified time
  Cancellable setTimeout(int time, Serializable timeout) {
    return getContext().system().scheduler().scheduleOnce(
        Duration.create(time, TimeUnit.MILLISECONDS),
        getSelf(),
        timeout, // the message to send
        getContext().system().dispatcher(), getSelf());
  }

  // fix the final decision of the current node
  void commitDecision(Integer v, EpochSeqNum epochSeqNum) {
    if (!hasDecided(epochSeqNum)) {
      currentValue = v;
      updateHistory.put(epochSeqNum, v);

      print("Committed value " + currentValue);
      print("Update History " + updateHistory.toString());
    }
  }

  boolean hasDecided(EpochSeqNum epochSeqNum) {
    return this.updateHistory.get(epochSeqNum) != null;
  } // has the node decided?

  // a simple logging function
  void print(String s) {
    System.out.format("%2d: %s\n", id, s);
  }

  @Override
  public Receive createReceive() {

    // Empty mapping: we'll define it in the inherited classes
    return receiveBuilder().build();
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
      if (key.currentEpoch == esn.currentEpoch && key.seqNum == esn.seqNum) {
        return entry.getValue();
      }
    }
    return null;
  }
}