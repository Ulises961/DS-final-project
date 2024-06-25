package org.total_order_broadcast;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Cancellable;

import scala.concurrent.duration.Duration;
import scala.util.Random;

import org.slf4j.LoggerFactory;
import org.slf4j.Logger;
import org.slf4j.MDC;

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

  protected Set<ActorRef> currentView;

  // each view has is assocaited w/ an Epoch
  protected final Map<EpochSeqNum, Set<ActorRef>> proposedView;

  // last sequence number for each node message (to avoid delivering duplicates)
  protected final Map<ActorRef, Integer> membersSeqno;

  // history of updates
  protected Map<EpochSeqNum, Integer> updateHistory;

  // deferred messages (of a future view)
  protected final Set<WriteDataMsg> deferredMsgSet;

  // cancellation of the heartbeat timeout
  protected Cancellable heartbeatTimeout;

  protected Cancellable electionTimeout;

  // to include delays in the messages
  private Random rnd = new Random();

  // TODO missed updates message to bring replicas up to date, from the
  // coordinator
  protected  Map<Integer, Set<ActorRef>> flushes;
  // Hearbeat
  protected final int HEARTBEAT_TIMEOUT_DURATION = 2000;
  protected final int HEARTBEAT_INTERVAL = 1000;

  final static int N_PARTICIPANTS = 3;
  final static int VOTE_TIMEOUT = 500; // timeout for the votes, ms
  final static int DECISION_TIMEOUT = 1000; // timeout for the decision, ms
  final static int RANDOM_DELAY = Math.round((VOTE_TIMEOUT + 50) / N_PARTICIPANTS); // timeout for the decision, ms

  protected Logger logger; 
  protected Map<String, String> contextMap;

  public Node(int id) {
    super();
    this.id = id;
    this.epochSeqNumPair = new EpochSeqNum(0, 0);
    this.group = new HashSet<>();
    this.currentView = new HashSet<>();
    this.proposedView = new HashMap<>();
    this.membersSeqno = new HashMap<>();
    this.deferredMsgSet = new HashSet<>();
    this.quorum = (N_PARTICIPANTS / 2) + 1;
    updateHistory = new HashMap<>();
    updateHistory.put(epochSeqNumPair, 0);
    logger = LoggerFactory.getLogger(Node.class);
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

  // TODO change this according to the new implementation of ESN, also the paper only says that the nodes share the UPDATED value
  public static class WriteOk implements Serializable {
    public final Integer value;
    public final EpochSeqNum epochSeqNum;
    public final ActorRef proposer;

    public WriteOk(Integer v, EpochSeqNum epochSeqNum, ActorRef proposer) {
      value = v;
      this.epochSeqNum = epochSeqNum;
      this.proposer =  proposer;
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

  public static class Recovery implements Serializable {}

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

  public static class ElectionAck implements Serializable {}

  public static class Heartbeat extends Timeout {}

  public static class HeartbeatTimeout extends Timeout {}

  public static class ElectionTimeout extends Timeout {
    int next;
    Map<Integer,Set<ActorRef>> flushes;
    public ElectionTimeout(int next, Map<Integer,Set<ActorRef>> flushes) {
      this.next = next;
      this.flushes = flushes;
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

  public static class PendingWriteMsg extends WriteDataMsg {
    public PendingWriteMsg(Integer value, ActorRef sender, boolean shouldCrash) {
      super(value, sender, shouldCrash);
    }

    public PendingWriteMsg(Integer value, ActorRef sender) {
      super(value, sender);
    }
  }

  public static class ViewChangeMsg implements Serializable {
    public final EpochSeqNum esn;
    public final Set<ActorRef> proposedView;

    public ViewChangeMsg(EpochSeqNum esn, Set<ActorRef> proposedView) {
      this.esn = esn;
      this.proposedView = Collections.unmodifiableSet(new HashSet<>(proposedView));
    }
  }

  public static class CrashMsg implements Serializable {}
  public static class ICMPRequest implements Serializable {}
  public static class ICMPResponse implements Serializable {}
  public static class FlushMsg implements Serializable {}

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

  public void setGroup(JoinGroupMsg sm) {
    participants = new ArrayList<>();
    for (ActorRef b : sm.group) {
      this.participants.add(b);
    }

    logWithMDC(contextMap, "Starting with " + sm.group.size() + " peer(s)");
    logWithMDC(contextMap, "Participants: " + participants.toString());
  }

  // emulate a crash and a recovery in a given time
  public void crash() {
    getContext().become(crashed());
    print("CRASH!!!");
  }

  public Receive crashed() {
    return receiveBuilder()
            .matchAny(msg -> {})
            .build();
  }

  // emulate a delay of d milliseconds
  public void delay(int d) {
    try {
      Thread.sleep(d);
    } catch (Exception ignored) {
    }
  }

  public void multicast(Serializable m) {
    for (ActorRef p : participants) {
      delay(RANDOM_DELAY);
      p.tell(m, getSelf());

    }
  }

  // a multicast implementation that crashes after sending the first message
  public void multicastAndCrash(Serializable m) {
    for (ActorRef p : participants) {
      delay(RANDOM_DELAY);
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
  public void commitDecision(Integer v, EpochSeqNum epochSeqNum) {
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

  // a simple logging function
  public void print(String s) {
    System.out.format("%2d: %s\n", id, s);
  }

  @Override
  public Receive createReceive() {

    // Empty mapping: we'll define it in the inherited classes
    return receiveBuilder().build();
  }

    // Utility method for logging with MDC context
    protected void logWithMDC(Map<String, String> contextMap, String message) {
        try {
            LogLevel level = LogLevel.INFO;
            // Set MDC context
            contextMap.forEach(MDC::put);

            // Log the message at the specified level
            switch (level) {
                case INFO:
                    logger.info(message);
                    break;
                case WARN:
                    logger.warn(message);
                    break;
                case ERROR:
                    logger.error(message);
                    break;
                case DEBUG:
                    logger.debug(message);
                    break;
            }
        } finally {
            // Clear MDC context
            MDC.clear();
        }
    }

    // Define LogLevel as an enum for convenience
    enum LogLevel {
        INFO, WARN, ERROR, DEBUG
    }
}