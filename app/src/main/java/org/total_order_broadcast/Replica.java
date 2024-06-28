package org.total_order_broadcast;

import java.io.Serializable;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.LoggerFactory;

import akka.actor.ActorRef;
import akka.actor.Cancellable;
import akka.actor.Props;



public class Replica extends Node {

  // Keeps track of who is the coordinator
  private ActorRef supervisor = null;

  // The propsed coordinator for the next view
  private ActorRef proposedCoord = null;

  // The coordinator id proposed in the election message
  private int proposedCoordinatorID;

  // In election ring, the next replica might have crashed
  private boolean nextHopTimedOut = false;
  
  // Next Replica id to send the election message
  private int nextHop = -1;

  //In the election ring, a replica has already received an election message
  private boolean hasReceivedElectionMessage = false;
  
  // In the election mode, the replicas defer messages until the new view is established
  private boolean deferringMessages = false;

  // Keeps trackof the request number for the current epoch
  private Integer currentRequest = 0;
  
  // Replica requests that have not been yet put to vote
  private List<WriteDataMsg> pendingMsg;
  
  // Unordered writes remain pending until the sequence is correct
  private Map<EpochSeqNum, WriteOk> unstableWrites;
  
  // Coordinator keeps track of pending requests associated to a Replica
  private Map<EpochSeqNum, ActorRef> pendingRequests;
  
  private Cancellable updateTimeOut;
  
  // Coordinator keeps track if a request has reached quorum to improve liveliness
  private Map<EpochSeqNum, Boolean> requestHasQuorum;

  // Coordinator keeps track of received acks for a request
  private Map<EpochSeqNum, Set<ActorRef>> receivedAcks;
  
  //Flushes for each epoch
  protected final Map<Integer, Set<ActorRef>> flushes;

  // Flushed replicas for the current epoch
  private Set<ActorRef> flushedReplicas;
  // Election timeout
  private boolean setElectionTimeout;

  public Replica(int id) {
    super(id);
    receivedAcks = new HashMap<>();
    this.pendingRequests = new HashMap<>();
    this.pendingMsg = new LinkedList<>();
    this.unstableWrites = new HashMap<>();
    logger = LoggerFactory.getLogger(Replica.class);
    requestHasQuorum = new HashMap<>();
    contextMap = new HashMap<>();
    contextMap.put("replicaId", String.valueOf(id));
    flushes = new HashMap<>();
    log("Replica " + id + " created", LogLevel.INFO);
  }

  @Override
  protected void onRecovery(Recovery msg) {
    return; // An Actor needs to implement an onRecovery method./
  }

  static public Props props(int id) {
    return Props.create(Replica.class, () -> new Replica(id));
  }

  public void onCrash(CrashMsg msg){
    crash();
  }
  // election message, this is what each replica receives/sends
  public static class ElectionMessage implements Serializable {
    public ActorRef proposedCoordinator;
    Map<EpochSeqNum, Integer> updateHistory;
    int proposedCoordinatorID;
    // Collected active replicas
    protected final Map<Integer, Set<ActorRef>> activeReplicas;

    public ElectionMessage(Map<EpochSeqNum, Integer> updateHistory, ActorRef coord, int proposedCoordinatorID, Map<Integer, Set<ActorRef>> activeReplicas) {
      this.updateHistory = updateHistory;
      this.proposedCoordinatorID = proposedCoordinatorID;
      this.proposedCoordinator = coord;
      this.activeReplicas = activeReplicas;
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
      .match(Heartbeat.class, this::onHeartbeat)
      .match(HeartbeatTimeout.class, this::onHeartbeatTimeout)
      .match(ICMPRequest.class, this::onPing)
      .match(CrashMsg.class, this::onCrash)
      .match(ElectionMessage.class, this::onElectionMessageReceipt)
      .match(ReadHistory.class, this::onReadHistory)
      .matchAny(msg -> log("Ignoring " + msg.getClass().getSimpleName() + " (normal mode)", LogLevel.DEBUG))
      .build();
  }
      
  public Receive electionMode() {
    return receiveBuilder()
      .match(Heartbeat.class, this::onHeartbeat)
      .match(HeartbeatTimeout.class, this::onHeartbeatTimeout)
      .match(RestartElection.class, this::onRestartElection)
      .match(ElectionTimeout.class, this::onElectionTimeout)
      .match(ElectionAck.class, this::onElectionAck)
      .match(ElectionMessage.class, this::onElectionMessageReceipt)
      .match(WriteDataMsg.class, this::onUpdateMessage)
      .match(PendingWriteMsg.class, this::onPendingWriteMessage)
      .match(SyncMessage.class, this::onSyncMessageReceipt)
      .match(UpdateRequest.class, this::onUpdateRequest)
      .match(UpdateAck.class, this::onUpdateAck)
      .match(WriteOk.class, this::onWriteOk)
      .match(FlushMsg.class, this::onFlushMessage)
      .match(ViewChangeMsg.class, this::onViewChange)
      .match(CrashMsg.class, this::onCrash)
      .matchAny(msg -> log("Ignoring " + msg.getClass().getSimpleName() + " (election mode)", LogLevel.DEBUG))
      .build();
  }
  
  public void onStartMessage(JoinGroupMsg msg) {
    setGroup(msg);
    coordinator = msg.coordinator;
    supervisor = msg.supervisor;
    if (isCoordinator()) {
      multicast(new Heartbeat());
      supervisor.tell(new Client.SetCoordinator(), getSelf());
    }
  }
  

  /**
   ************************************* COMMUNICATION HANDLING *************************************
   */
  
   public void onTimeout(UpdateTimeOut msg) {
    // If the coordinator does not respond, the replica starts an election
    log("Timeout for update request: " + msg.epochSeqNum, LogLevel.INFO);
    onHeartbeatTimeout(new HeartbeatTimeout());
  }
  
  public void onReadMessage(ReadDataMsg msg) { /* Value read from Client */
    msg.sender.tell(new DataMsg(getValue()), getSelf());
    log("Read from client: " + getSender().path().name() + ". Value returned: " + currentValue, LogLevel.DEBUG);
  }

  public void onHeartbeat(Heartbeat msg) {
    if (isCoordinator()) {
      multicastExcept(new Heartbeat(), getSelf());
      heartbeat = setTimeout(HEARTBEAT_INTERVAL, new Heartbeat());
    } else {
      renewHeartbeatTimeout();
    }
  }

  public void onPing(ICMPRequest msg) {
    log("Received ping from " + getSender().path().name(), LogLevel.DEBUG);
    getSender().tell(new ICMPResponse(), getSelf());
  }

  private void renewHeartbeatTimeout(){
    if(heartbeatTimeout != null) {
      heartbeatTimeout.cancel();
    }
    if(!isCoordinator()){
      heartbeatTimeout = setTimeout(HEARTBEAT_TIMEOUT_DURATION, new HeartbeatTimeout());
    }
  }

  /**
   ************************************* UPDATES HANDLING *************************************
   */

  /* Value update from Client */
  public void onUpdateMessage(WriteDataMsg msg) {
    // Defer messages during election. New messages make part of the next epoch
    if(deferringMessages) {
      deferredMsgSet.add(msg);
      log("Deferring message: " + msg.value, LogLevel.INFO);
    } else {
      if (isCoordinator()) {
        log("Received Write request from " + getSender().path().name() + " with value " + msg.value, LogLevel.INFO);
        
        // If the sender in the message is different from the sender of the message
        // it was forwarded by a replica
        if(getSender() != msg.sender){
          requestUpdate(msg.value, getSender());
        } else {
          requestUpdate(msg.value, getSelf());
        }

      } else {
        log("Received update message from client " + msg.sender.path().name() + " with value: " + msg.value, LogLevel.INFO);
        log("Forwarding update message to coordinator: " + coordinator.path().name() + " with value " + msg.value, LogLevel.DEBUG);
        // forward request to the coordinator, do not propagate the shouldCrash flag
        coordinator.tell(new WriteDataMsg(msg.value, getSelf()), getSelf());
        
        // Keep message in memory as pending
        pendingMsg.add(msg);
        log("Pending message: " + msg.value, LogLevel.DEBUG);

        // Set a timeout for the update request
        if(updateTimeOut != null) {
          updateTimeOut.cancel();
        }
        updateTimeOut = setTimeout(DECISION_TIMEOUT, new UpdateTimeOut(epochSeqNumPair));
      }
      

      // The crash occurs only to the client's server
      if(msg.shouldCrash){
        crash();
      }
    }
  }

  // CO-HORTS RECEIVE THIS AND SEND ACKS BACK TO THE COORDINATOR
  public void onUpdateRequest(UpdateRequest msg) {
    // Updates must be monotonically increasing within the latest epoch
    log("Received update request from coordinator. Value: " + msg.value + " SeqNum: " + msg.epochSeqNum, LogLevel.INFO);
    coordinator.tell(new UpdateAck(msg.value, msg.epochSeqNum), getSelf());
    
    if(updateTimeOut != null) {
      updateTimeOut.cancel();
    }

    // Wait for a WriteOk from the coordinator 
    updateTimeOut = setTimeout(DECISION_TIMEOUT, new UpdateTimeOut(msg.epochSeqNum));
    
    // Assume the heartbeat is received
    renewHeartbeatTimeout();
  }

  // COORDINATOR RECEIVES ACKS AND SENDS WRITEOK
  public void onUpdateAck(UpdateAck ack) {
    log("Is Ack" + ack.epochSeqNum + " expected: " + isAckExpected(ack.epochSeqNum), LogLevel.INFO);
    if (isAckExpected(ack.epochSeqNum)) {
      ActorRef sender = getSender();
      Set<ActorRef> voters = receivedAcks.get(ack.epochSeqNum);
      voters.add(sender);
      if (voters.size() >= quorum) { // enough acks received, now send WRITEOK message
        log("Quorum reached, sending Ok for value " + ack.value, LogLevel.INFO);
        ActorRef proposer = pendingRequests.get(ack.epochSeqNum);
        multicast(new WriteOk(ack.value, ack.epochSeqNum, proposer));
        requestHasQuorum.put(ack.epochSeqNum,true); // update phase completed, no more acks expected
      }
    }
  }

  // ALL'S WELL THAT ENDS WELL: COMMIT UPDATE
  public void onWriteOk(WriteOk msg) {
    
    // Cancel the timeout for the update request
    updateTimeOut.cancel();

    // Assume the heartbeat is received
    renewHeartbeatTimeout();

    // Check if the message is the next in sequence
    if(msg.epochSeqNum.seqNum == epochSeqNumPair.seqNum + 1){
      log("Committing value: " + msg.value + " SeqNum: " + msg.epochSeqNum.seqNum, LogLevel.INFO);

      // store the decision
      commitDecision(msg.value, msg.epochSeqNum);

      if(isCoordinator()){
        pendingRequests.remove(msg.epochSeqNum);
      }

      // Check if there are any pending writes that can now be committed
      while(unstableWrites.containsKey(new EpochSeqNum(epochSeqNumPair.currentEpoch, epochSeqNumPair.seqNum + 1))){
        WriteOk writeOk = unstableWrites.get(new EpochSeqNum(epochSeqNumPair.currentEpoch, epochSeqNumPair.seqNum + 1));
        
        log("Committing deferred value: " + writeOk.value + " SeqNum: " + writeOk.epochSeqNum.seqNum, LogLevel.INFO);
        
        commitDecision(writeOk.value, writeOk.epochSeqNum);
        unstableWrites.remove(writeOk.epochSeqNum);
      }

    } else if(msg.epochSeqNum.seqNum >= epochSeqNumPair.seqNum){
      log("Message already committed " + msg.value + " seq num " + msg.epochSeqNum, LogLevel.DEBUG);
    } else {
      log("Defering commit of value: " + msg.value + " SeqNum: " + msg.epochSeqNum.seqNum + " current epoch seqNum: " + epochSeqNumPair.seqNum, LogLevel.DEBUG);
      
      // Defer commit until the correct sequence number is reached
      unstableWrites.put(msg.epochSeqNum, msg);
    } 

    // Remove the message from the pending list
    if(msg.proposer.equals(getSelf())){
      Iterator<WriteDataMsg> iterator = pendingMsg.iterator();
      log("Pending messages: " + pendingMsg, LogLevel.INFO);
      
      while (iterator.hasNext()) {
        WriteDataMsg currentMsg = iterator.next();
        if (currentMsg.value == msg.value) {
          log("Removing pending message: " + msg.value + " SeqNum: " + msg.epochSeqNum.seqNum, LogLevel.INFO);
          iterator.remove();
          break; // Exit the loop after removing the first match
        }
      }
      
      log("Remaining messages: " + pendingMsg, LogLevel.INFO);
     
      if (isCoordinator()) {
        // Remove the message from the pending requests list
        pendingRequests.remove(msg.epochSeqNum);
      } 

      log("Is pending message empty: " + pendingMsg.isEmpty(), LogLevel.DEBUG);
      // After all messages have been committed, send a flush message to change the view
      if(pendingMsg.isEmpty()){
        log("All pending messages have been committed, tell coordinator " + coordinator.path().name(), LogLevel.DEBUG);
        coordinator.tell(new FlushMsg(), getSelf());

      }
    } 
  }

  private boolean isAckExpected(EpochSeqNum epochSeqNum) {
    return !requestHasQuorum.get(epochSeqNum);
  }
  
  // USED BY THE COORD TO MULTICAST THE UPDATE REQUEST
  private void requestUpdate(Integer value, ActorRef sender) {
    currentRequest++;
    EpochSeqNum esn = new EpochSeqNum(epochSeqNumPair.currentEpoch, currentRequest);
    Set<ActorRef> acks = new HashSet<>();
    receivedAcks.put(esn, acks);
    requestHasQuorum.put(esn, false);
    log("Requesting update from replicas. Value proposed: " + value + " SeqNum: " + esn, LogLevel.INFO);
    multicast(new UpdateRequest(value, esn));

    // Store in memory which replica initiated the request. 
    // The writeOk includes the replica that initiated the request
    // The replica will remove the message from its pending list once the writeOk is received
    pendingRequests.put(esn, sender);
  }
  


  /**
   ****************************** ELECTION METHODS AND CLASSES ********************************
   */

  // COORDINATOR CRASHED, THIS NODE IS THE ONE WHICH DETECTED THE CRASH, THUS THE INITIATOR OF THE ELECTION ALGO
  public void onHeartbeatTimeout(HeartbeatTimeout msg) {
    log("Coordinator is not responding. Starting election.", LogLevel.DEBUG);

    getContext().become(electionMode());
    deferringMessages = true;

    startElection();
  }

  public void onElectionMessageReceipt(ElectionMessage electionMessage){

    log("Received proposed coordinator: " + electionMessage.proposedCoordinatorID + " from sender " + getSender().path().name(), LogLevel.INFO);

    // send ack to whoever sent the message
    if (hasReceivedElectionMessage && !hasBeenUpdated(electionMessage)){
      // END ELECTION
      updateCoordinator(proposedCoord);
      cleanUp();
      voteTimeout.cancel();
      if (isCoordinator()) {
        log("Coordinator in the new view: " + coordinator.path().name(), LogLevel.INFO);
        deferringMessages = false;
        Set<ActorRef> activeReplicas = electionMessage.activeReplicas.get(epochSeqNumPair.currentEpoch);
        log("New view established " + activeReplicas, LogLevel.INFO);
        updateQuorum(activeReplicas.size());
        this.proposedView.put(epochSeqNumPair.currentEpoch, activeReplicas);
        flushes.put(epochSeqNumPair.currentEpoch, new HashSet<>());
        flushedReplicas = this.flushes.get(this.epochSeqNumPair.currentEpoch) == null ? this.flushes.get(this.epochSeqNumPair.currentEpoch) : new HashSet<>();

        multicast(new SyncMessage(updateHistory));
        multicast(new Heartbeat());
      } else {
        // set timeout in case new coordinator also crashed
        heartbeatTimeout = setTimeout(HEARTBEAT_TIMEOUT_DURATION, new HeartbeatTimeout());
      }
    } else {
      // Someone else has discovered a crashed coordinator, no need to trigger another election
      if(heartbeatTimeout != null){
        heartbeatTimeout.cancel();
      }

      deferringMessages = true;
      hasReceivedElectionMessage = true;
      
      getContext().become(electionMode());
      
      getSender().tell(new ElectionAck(), getSelf());
      
      electionMessage.activeReplicas.get(epochSeqNumPair.currentEpoch).add(getSelf());

      // view update content
      proposedCoord = electionMessage.proposedCoordinator;
      proposedCoordinatorID = electionMessage.proposedCoordinatorID;
      updateCoordinator(proposedCoord);

      List<EpochSeqNum> epochSeqNumList = new LinkedList<>(electionMessage.updateHistory.keySet());
      
      Map<EpochSeqNum, Integer> update = electionMessage.updateHistory;
      // sorting epochs-seqNums in decreasing order;
      epochSeqNumList.sort((o1, o2) -> o1.currentEpoch < o2.currentEpoch ? 1 : -1);
      if (epochSeqNumList.get(0).getCurrentEpoch() > this.epochSeqNumPair.getCurrentEpoch()) {
        log("I'm behind, updating history", LogLevel.DEBUG);
        // if we're here we missed out 1+ epochs, we must update our history
        int diff = Math.abs(epochSeqNumList.get(0).getCurrentEpoch() - this.epochSeqNumPair.getCurrentEpoch());
        // since epoch updates are sequential we can iterate over the difference diff.
        int pos = 0;
        while (pos < diff) {
          // there's probably a better way to do this but I can't think of it rn :)
          // the idea is that we get the epochSeqNum from the List, then we obtain the value from the hashmap
          // and add those to our updateHistory
          EpochSeqNum key = epochSeqNumList.get(pos);
          Integer value = electionMessage.updateHistory.get(key);
          this.updateHistory.put(key, value);
          pos++;
        }

        // update self, not the message.
        updateCoordinator(electionMessage.proposedCoordinator);
      } else if (epochSeqNumList.get(0).getCurrentEpoch() < this.epochSeqNumPair.getCurrentEpoch()) {
        // we have the latest version so far, we must update the message and forward it.
        proposedCoord = getSelf();
        proposedCoordinatorID = this.id;
        update = this.updateHistory;

        log("I have the latest version", LogLevel.DEBUG);
      } else {
        // everything matches, break ties using node id
        proposedCoordinatorID = Math.max(proposedCoordinatorID, this.id);
        proposedCoord = proposedCoordinatorID == this.id ? getSelf() : proposedCoord;
        log("Tie breaker: " + proposedCoordinatorID, LogLevel.DEBUG);
      }

      log("Updated proposed coordinator: " + proposedCoordinatorID + " message coordinator " + electionMessage.proposedCoordinatorID + " next hop " + nextHop, LogLevel.DEBUG);
      
      nextHop = sendElectionMsg(new ElectionMessage(update, proposedCoord, proposedCoordinatorID, electionMessage.activeReplicas), nextHop);

      if(voteTimeout != null){
        voteTimeout.cancel();
      }

      voteTimeout = setTimeout(VOTE_TIMEOUT, new ElectionTimeout(nextHop, electionMessage.activeReplicas));
    }
  }
  
  public void onSyncMessageReceipt(SyncMessage sm){
    this.updateHistory = new HashMap<>(sm.updateHistory);
    deferringMessages = false;
          
    if(electionTimeout != null){
      log("Cancelling election timeout", LogLevel.INFO);
      electionTimeout.cancel();
      setElectionTimeout = false;
    }

    if(voteTimeout != null){
      log("Cancelling vote timeout", LogLevel.INFO);
      voteTimeout.cancel();
    }
    
    if(pendingMsg.size() > 0){
      log("Solve pending messages", LogLevel.INFO);
      // Retry pending messages from current epoch
      for(WriteDataMsg msg : pendingMsg){
        coordinator.tell(new PendingWriteMsg(msg.value, msg.sender), getSelf());
      }
    } else {
      log("No pending messages", LogLevel.INFO);
      coordinator.tell(new FlushMsg(), getSelf());
    }
  
  }

  public void onFlushMessage(FlushMsg msg) {
    log("Received flush message from replica " + getSender().path().name(), LogLevel.INFO);

    if (isCoordinator()) {
      Set<ActorRef> participants = proposedView.get(epochSeqNumPair.currentEpoch);
      flushedReplicas.add(getSender());
      log("Flushed replicas: " + flushedReplicas.toString(), LogLevel.DEBUG);
      log("Can propose view: " + (flushedReplicas.size() >= participants.size()), LogLevel.DEBUG);
      if (flushedReplicas.size() >= participants.size()) {
        multicast(new ViewChangeMsg(new EpochSeqNum(epochSeqNumPair.currentEpoch + 1, 0), participants, coordinator));
        currentRequest = 0;
        log("View change message sent", LogLevel.INFO);
      }
    }
  }

  public void onViewChange(ViewChangeMsg msg) {
    currentView.clear();
    epochSeqNumPair = msg.esn;

    for(ActorRef participant : msg.proposedView){
      currentView.add(participant);
    }


    log( "Participants in the new view: " + currentView.toString(), LogLevel.INFO);
    log( "Coordinator in the new view: " + coordinator.path().name(), LogLevel.INFO);
    getContext().become(createReceive());
    
    if (isCoordinator()) {
      supervisor.tell(new Client.SetCoordinator(), getSelf());
    }
  }
  
  public void onElectionAck(ElectionAck ack) {
    voteTimeout.cancel();
    // eventually this will be set to false (assumption that there is always at least a quorum
    nextHopTimedOut = false;
  }

  // SOLVE PENDING MESSAGES FROM THE REPLICA BEFORE CHANGING VIEW
  public void onPendingWriteMessage(PendingWriteMsg msg) {
    if (isCoordinator()) {
      requestUpdate(msg.value, getSender());
    }
  }


  // if nextHop has crashed, we try with the node after that, until someone sends back an Ack
  public void onElectionTimeout(ElectionTimeout msg) {
    nextHopTimedOut = true;
    // because nextHop timedout we try with the one after that: nextHop+1;
    nextHop = sendElectionMsg(new ElectionMessage(updateHistory,getSelf(),this.id, msg.activeReplicas),msg.next);
    // set timeout with nextHop+1
    voteTimeout = setTimeout(VOTE_TIMEOUT, new ElectionTimeout(nextHop, msg.activeReplicas));
    log("Election timeout. Trying with next node: " + nextHop, LogLevel.INFO);
  } 
  
  public void onRestartElection(RestartElection msg) {
    log("Restarting election", LogLevel.INFO);
    setElectionTimeout = false;
    nextHop = -1;
    startElection();
  }

  private void startElection() {
     // active replicas are added to the flushes during the election, 
    //these will become the participants in the new epoch
    Map<Integer, Set<ActorRef>> replicasInEpoch = new HashMap<>();
    Set<ActorRef> activeReplicas = new HashSet<>();
    activeReplicas.add(getSelf());
    replicasInEpoch.put(epochSeqNumPair.currentEpoch, activeReplicas);
    nextHop = sendElectionMsg(new ElectionMessage(updateHistory, getSelf(), this.id, replicasInEpoch), nextHop);
    // because of the assumption that there will ALWAYS be a quorum we can safely say that
    // next hop will NEVER be this node
    voteTimeout = setTimeout(VOTE_TIMEOUT, new ElectionTimeout(nextHop, replicasInEpoch));

    // if(!setElectionTimeout){
    //   log("Setting election timeout", LogLevel.INFO);
    //   electionTimeout = setTimeout(ELECTION_TIMEOUT, new RestartElection());
    //   setElectionTimeout = true;
    // }
  }

  /**
   *  Logic: sort list of participants, get replica whose id > this.id, then try to send, if timeout update nextHop
   */
  private int sendElectionMsg(ElectionMessage electionMessage, int next){
    List<ActorRef> participants = new LinkedList<ActorRef>(currentView);
    Collections.sort(participants, (a, b) -> a.path().name().compareTo(b.path().name()));

    if (next == -1) {
      int pos = participants.indexOf(getSelf());
      participants.get((pos + 1) % participants.size()).tell(electionMessage, getSelf());
      return (pos + 1) % participants.size();
    } else if (nextHopTimedOut){
      participants.get((next+1) % participants.size()).tell(electionMessage, getSelf());
      return (next + 1) % participants.size();
    } else {
      participants.get((next) % participants.size()).tell(electionMessage, getSelf());
      return next % participants.size();
    }
  }

  private void updateCoordinator(ActorRef coordinator){
    this.coordinator = coordinator;
  }

  private boolean hasBeenUpdated(ElectionMessage msg){
    return !msg.updateHistory.equals(this.updateHistory) || msg.proposedCoordinatorID != this.proposedCoordinatorID;
  }

  public void onReadHistory(ReadHistory msg) {
    log("History: " + updateHistory.toString(), LogLevel.INFO);
  }
  
  private void cleanUp(){
    this.proposedCoord = null;
    this.proposedCoordinatorID = -1;
  }

  private boolean isCoordinator() {
    if (this.coordinator == null) {
      logger.error("Coordinator is not set");
      return false;
    }
    return coordinator.equals(getSelf());
  }

}
