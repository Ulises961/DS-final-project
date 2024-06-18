package org.total_order_broadcast;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Cancellable;
import scala.concurrent.duration.Duration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public abstract class Node extends AbstractActor {
  protected static final Logger LOGGER = LoggerFactory.getLogger(Node.class);
  
  protected int id; // node ID

  protected List<ActorRef> participants; // list of participant nodes



  // TODO missed updates message to bring replicas up to date, from the
  // coordinator

  protected ActorRef coordinator;
  // Hearbeat
  protected final int HEARTBEAT_TIMEOUT_DURATION = 2000;
  protected final int HEARTBEAT_INTERVAL = 1000;

  final static int N_PARTICIPANTS = 3;
  final static int VOTE_TIMEOUT = 500; // timeout for the votes, ms
  final static int DECISION_TIMEOUT = 1000; // timeout for the decision, ms
  final static int RANDOM_DELAY = Math.round((VOTE_TIMEOUT + 50) / N_PARTICIPANTS); // timeout for the decision, ms

  public Node(int id) {
    super();
    this.id = id;
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

  public static class Heartbeat extends Timeout {}

  public static class HeartbeatTimeout extends Timeout {}

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


  // abstract method to be implemented in extending classes
  protected abstract void onRecovery(Recovery msg);

  void setGroup(JoinGroupMsg sm) {
    participants = new ArrayList<>();
    for (ActorRef b : sm.group) {
      this.participants.add(b);
    }
    print("Starting with " + sm.group.size() + " peer(s)");
    LOGGER.info("This is an INFO level log message");
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

  // schedule a Timeout message in specified time
  Cancellable setTimeout(int time, Serializable timeout) {
    return getContext().system().scheduler().scheduleOnce(
        Duration.create(time, TimeUnit.MILLISECONDS),
        getSelf(),
        timeout, // the message to send
        getContext().system().dispatcher(), getSelf());
  }


  // a simple logging function
  void print(String s) {
    System.out.format("%2d: %s\n", id, s);
  }

  @Override
  public Receive createReceive() {

    // Empty mapping: we'll define it in the inherited classes
    return receiveBuilder().build();
  }
 
}