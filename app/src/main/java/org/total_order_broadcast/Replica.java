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
    log("Replica " + id + " created", Cluster.LogLevel.INFO);
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

  /**
   * Represents an election message sent and received by each replica during the election process.
   * This class contains the proposed coordinator, update history, proposed coordinator ID, and active replicas.
   */
  public static class ElectionMessage implements Serializable {
    public ActorRef proposedCoordinator;
    Map<EpochSeqNum, Integer> updateHistory;
    int proposedCoordinatorID;
    // Collected active replicas
    protected final Map<Integer, Set<ActorRef>> activeReplicas;

    /**
     * Constructs an {@code ElectionMessage} with the specified parameters.
     *
     * @param updateHistory the update history as a map of {@code EpochSeqNum} to {@code Integer}.
     * @param coord the proposed coordinator {@code ActorRef}.
     * @param proposedCoordinatorID the ID of the proposed coordinator.
     * @param activeReplicas the map of active replicas collected during the election process.
     */
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
      .matchAny(msg -> log("Ignoring " + msg.getClass().getSimpleName() + " (normal mode)", Cluster.LogLevel.DEBUG))
      .build();
  }
      
  public Receive electionMode() {
    return receiveBuilder()
      .match(Heartbeat.class, this::onHeartbeat)
      .match(HeartbeatTimeout.class, this::onHeartbeatTimeout)
      .match(RestartElection.class, this::onRestartElection)
      .match(ElectionRestated.class, this::onElectionRestarted)
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
      .matchAny(msg -> log("Ignoring " + msg.getClass().getSimpleName() + " (election mode)", Cluster.LogLevel.DEBUG))
      .build();
  }

  /**
   * Handles the start message to join a group.
   * This method sets the group, assigns the coordinator and supervisor, and sends a heartbeat if the current node is the coordinator.
   *
   * @param msg the {@code JoinGroupMsg} received, containing group information, coordinator, and supervisor.
   */
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
   * Handles the timeout for an update request.
   * If the coordinator does not respond, this method starts an election process.
   *
   * @param msg the {@code UpdateTimeOut} received, indicating the timeout for an update request.
   */
   public void onTimeout(UpdateTimeOut msg) {
    // If the coordinator does not respond, the replica starts an election
    log("Timeout for update request: " + msg.epochSeqNum, Cluster.LogLevel.INFO);
    onHeartbeatTimeout(new HeartbeatTimeout());
  }

  /**
   * Handles a read message from a client.
   * This method sends the current value back to the client.
   *
   * @param msg the {@code ReadDataMsg} received, containing the sender and read request details.
   */
  public void onReadMessage(ReadDataMsg msg) { /* Value read from Client */
    msg.sender.tell(new DataMsg(getValue()), getSelf());
    log("Read from client: " + getSender().path().name() + ". Value returned: " + currentValue, Cluster.LogLevel.DEBUG);
  }

  /**
   * Handles the receipt of a heartbeat message.
   * If the current node is the coordinator, it multicasts a heartbeat to other nodes.
   * Otherwise, it renews the heartbeat timeout.
   *
   * @param msg the {@code Heartbeat} message received.
   */
  public void onHeartbeat(Heartbeat msg) {
    if (isCoordinator()) {
      multicastExcept(new Heartbeat(), getSelf());
      heartbeat = setTimeout(HEARTBEAT_INTERVAL, new Heartbeat());
    } else {
      renewHeartbeatTimeout();
    }
  }

  /**
   * Handles the receipt of a ping message.
   * This method logs the receipt of the ping and sends a response back to the sender.
   *
   * @param msg the {@code ICMPRequest} message received, indicating a ping request.
   */
  public void onPing(ICMPRequest msg) {
    log("Received ping from " + getSender().path().name(), Cluster.LogLevel.DEBUG);
    getSender().tell(new ICMPResponse(), getSelf());
  }

  /**
   * Cancels the current heartbeat timeout and schedules a new one if not the coordinator.
   */
  private void renewHeartbeatTimeout(){
    if(heartbeatTimeout != null) {
      heartbeatTimeout.cancel();
    }
    if(!isCoordinator()){
      heartbeatTimeout = setTimeout(HEARTBEAT_TIMEOUT_DURATION, new HeartbeatTimeout());
    }
  }

  /**
   * Handles the WriteDataMsg message received from clients or replicas, processing write requests or updates.
   * Depending on the system state, it either processes the write request locally if the node is the coordinator,
   * forwards the update message to the coordinator if not, or defers messages during an election.
   *
   * @param msg The WriteDataMsg message containing the write/update details, including value and sender information.
   */
  public void onUpdateMessage(WriteDataMsg msg) {
    // Defer messages during election. New messages make part of the next epoch
    if(deferringMessages) {
      deferredMsgSet.add(msg);
      log("Deferring message: " + msg.value, Cluster.LogLevel.INFO);
    } else {
      if (isCoordinator()) {
        log("Received Write request from " + getSender().path().name() + " with value " + msg.value, Cluster.LogLevel.INFO);
        
        // If the sender in the message is different from the sender of the message
        // it was forwarded by a replica
        if(getSender() != msg.sender){
          requestUpdate(msg.value, getSender());
        } else {
          requestUpdate(msg.value, getSelf());
        }

      } else {
        log("Received update message from client " + msg.sender.path().name() + " with value: " + msg.value, Cluster.LogLevel.INFO);
        log("Forwarding update message to coordinator: " + coordinator.path().name() + " with value " + msg.value, Cluster.LogLevel.DEBUG);
        // forward request to the coordinator, do not propagate the shouldCrash flag
        coordinator.tell(new WriteDataMsg(msg.value, getSelf()), getSelf());
        
        // Keep message in memory as pending
        pendingMsg.add(msg);
        log("Pending message: " + msg.value, Cluster.LogLevel.DEBUG);

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

  /**
   * Handles the UpdateRequest message received from the coordinator, initiating an update process.
   * Sends an acknowledgement (UpdateAck) to the coordinator, sets a timeout for receiving a WriteOk,
   * and renews the heartbeat timeout assumption.
   *
   * @param msg The UpdateRequest message containing the update details, including value and epoch sequence number.
   */
  public void onUpdateRequest(UpdateRequest msg) {
    // Updates must be monotonically increasing within the latest epoch
    log("Received update request from coordinator. Value: " + msg.value + " SeqNum: " + msg.epochSeqNum, Cluster.LogLevel.INFO);
    coordinator.tell(new UpdateAck(msg.value, msg.epochSeqNum), getSelf());
    
    if(updateTimeOut != null) {
      updateTimeOut.cancel();
    }

    // Wait for a WriteOk from the coordinator 
    updateTimeOut = setTimeout(DECISION_TIMEOUT, new UpdateTimeOut(msg.epochSeqNum));
    
    // Assume the heartbeat is received
    renewHeartbeatTimeout();
  }

  /**
   * Handles the UpdateAck message received from replicas confirming receipt of an update request.
   * Processes acknowledgements to determine when a quorum is reached and sends a WriteOk message
   * to signify completion of the update phase for the corresponding epoch sequence number.
   *
   * @param ack The UpdateAck message containing the acknowledgement details, including the value,
   *            epoch sequence number, and sender information.
   */
  public void onUpdateAck(UpdateAck ack) {
    log("Is Ack" + ack.epochSeqNum + " expected: " + isAckExpected(ack.epochSeqNum), Cluster.LogLevel.INFO);
    if (isAckExpected(ack.epochSeqNum)) {
      ActorRef sender = getSender();
      Set<ActorRef> voters = receivedAcks.get(ack.epochSeqNum);
      voters.add(sender);
      if (voters.size() >= quorum) { // enough acks received, now send WRITEOK message
        log("Quorum reached, sending Ok for value " + ack.value, Cluster.LogLevel.INFO);
        ActorRef proposer = pendingRequests.get(ack.epochSeqNum);
        multicast(new WriteOk(ack.value, ack.epochSeqNum, proposer));
        requestHasQuorum.put(ack.epochSeqNum,true); // update phase completed, no more acks expected
      }
    }
  }

  /**
   * Handles the WriteOk message received from the coordinator or another node.
   * This method processes commit decisions based on the sequence numbers and manages pending messages.
   * If the received message's sequence number indicates it's the next expected in sequence, it commits
   * the associated value and updates internal state accordingly. If the sequence number is higher than
   * the current expected sequence number, it defers committing until the correct sequence is reached.
   * Pending messages associated with the same value are removed once committed.
   * This method also handles scenarios where the actor itself proposed the write operation. It removes
   * corresponding pending messages and updates internal state. If all pending messages are committed,
   * it notifies the coordinator to change the view.
   *
   * @param msg The WriteOk message containing the commit information, including the value, epoch sequence number,
   *            and proposer details.
   */
  public void onWriteOk(WriteOk msg) {
    
    // Cancel the timeout for the update request
    updateTimeOut.cancel();

    // Assume the heartbeat is received
    renewHeartbeatTimeout();

    // Check if the message is the next in sequence
    if(msg.epochSeqNum.seqNum == epochSeqNumPair.seqNum + 1){
      log("Committing value: " + msg.value + " SeqNum: " + msg.epochSeqNum.seqNum, Cluster.LogLevel.INFO);

      // store the decision
      commitDecision(msg.value, msg.epochSeqNum);

      if(isCoordinator()){
        pendingRequests.remove(msg.epochSeqNum);
      }

      // Check if there are any pending writes that can now be committed
      while(unstableWrites.containsKey(new EpochSeqNum(epochSeqNumPair.currentEpoch, epochSeqNumPair.seqNum + 1))){
        WriteOk writeOk = unstableWrites.get(new EpochSeqNum(epochSeqNumPair.currentEpoch, epochSeqNumPair.seqNum + 1));
        
        log("Committing deferred value: " + writeOk.value + " SeqNum: " + writeOk.epochSeqNum.seqNum, Cluster.LogLevel.INFO);
        
        commitDecision(writeOk.value, writeOk.epochSeqNum);
        unstableWrites.remove(writeOk.epochSeqNum);
      }

    } else if(msg.epochSeqNum.seqNum >= epochSeqNumPair.seqNum){
      log("Message already committed " + msg.value + " seq num " + msg.epochSeqNum, Cluster.LogLevel.DEBUG);
    } else {
      log("Defering commit of value: " + msg.value + " SeqNum: " + msg.epochSeqNum.seqNum + " current epoch seqNum: " + epochSeqNumPair.seqNum, Cluster.LogLevel.DEBUG);
      
      // Defer commit until the correct sequence number is reached
      unstableWrites.put(msg.epochSeqNum, msg);
    } 

    // Remove the message from the pending list
    if(msg.proposer.equals(getSelf())){
      Iterator<WriteDataMsg> iterator = pendingMsg.iterator();
      log("Pending messages: " + pendingMsg, Cluster.LogLevel.INFO);
      
      while (iterator.hasNext()) {
        WriteDataMsg currentMsg = iterator.next();
        if (currentMsg.value == msg.value) {
          log("Removing pending message: " + msg.value + " SeqNum: " + msg.epochSeqNum.seqNum, Cluster.LogLevel.INFO);
          iterator.remove();
          break; // Exit the loop after removing the first match
        }
      }
      
      log("Remaining messages: " + pendingMsg, Cluster.LogLevel.INFO);
     
      if (isCoordinator()) {
        // Remove the message from the pending requests list
        pendingRequests.remove(msg.epochSeqNum);
      } 

      log("Is pending message empty: " + pendingMsg.isEmpty(), Cluster.LogLevel.DEBUG);
      // After all messages have been committed, send a flush message to change the view
      if(pendingMsg.isEmpty()){
        log("All pending messages have been committed, tell coordinator " + coordinator.path().name(), Cluster.LogLevel.DEBUG);
        coordinator.tell(new FlushMsg(), getSelf());

      }
    } 
  }

  /**
   * Checks if an acknowledgement (Ack) is expected for a given epoch sequence number.
   * This method determines whether the system is still waiting for acknowledgements
   * to achieve a quorum for the specified epoch sequence number.
   *
   * @param epochSeqNum The epoch sequence number for which to check the acknowledgement status.
   * @return {@code true} if an Ack is expected (i.e., quorum has not been achieved);
   *         {@code false} if an Ack is not expected (i.e., quorum has been achieved).
   */
  private boolean isAckExpected(EpochSeqNum epochSeqNum) {
    return !requestHasQuorum.get(epochSeqNum);
  }

  /**
   * Increments the current request counter, prepares and sends an update request to replicas,
   * and manages necessary data structures for tracking acknowledgements and pending requests.
   *
   * @param value The integer value to be updated.
   * @param sender The actor reference of the sender initiating the update request.
   */
  private void requestUpdate(Integer value, ActorRef sender) {
    currentRequest++;
    EpochSeqNum esn = new EpochSeqNum(epochSeqNumPair.currentEpoch, currentRequest);
    Set<ActorRef> acks = new HashSet<>();
    receivedAcks.put(esn, acks);
    requestHasQuorum.put(esn, false);
    log("Requesting update from replicas. Value proposed: " + value + " SeqNum: " + esn, Cluster.LogLevel.INFO);
    multicast(new UpdateRequest(value, esn));

    // Store in memory which replica initiated the request. 
    // The writeOk includes the replica that initiated the request
    // The replica will remove the message from its pending list once the writeOk is received
    pendingRequests.put(esn, sender);
  }


   /**
   * Handles the HeartbeatTimeout message indicating that the coordinator is not responding,
   * initiating the election algorithm on this node, which detected the coordinator's absence.
   *
   * @param msg The HeartbeatTimeout message indicating the timeout event.
   */
  public void onHeartbeatTimeout(HeartbeatTimeout msg) {
    log("Coordinator is not responding. Starting election.", Cluster.LogLevel.DEBUG);

    getContext().become(electionMode());
    deferringMessages = true;

    startElection();
  }

  /**
   * Handles the receipt of an election message within the cluster.
   * This method processes the election message to update the coordinator,
   * manage the election state, and ensure the cluster remains synchronized.
   *
   * @param electionMessage the {@code ElectionMessage} received, containing information
   *                        about the proposed coordinator, active replicas, and update history.
   */
  public void onElectionMessageReceipt(ElectionMessage electionMessage){

    log("Received proposed coordinator: " + electionMessage.proposedCoordinatorID + " from sender " + getSender().path().name(), Cluster.LogLevel.INFO);

    // send ack to whoever sent the message
    if (hasReceivedElectionMessage && !hasBeenUpdated(electionMessage)){
      // END ELECTION
      updateCoordinator(proposedCoord);
      cleanUp();
      voteTimeout.cancel();
      if (isCoordinator()) {
        log("Coordinator in the new view: " + coordinator.path().name(), Cluster.LogLevel.INFO);
        deferringMessages = false;
        Set<ActorRef> activeReplicas = electionMessage.activeReplicas.get(epochSeqNumPair.currentEpoch);
        log("New view established " + activeReplicas, Cluster.LogLevel.INFO);
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
      // Every replica must have a chance to restart an election
      if(!setElectionTimeout){
        createElectionTimeout();
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
        log("I'm behind, updating history", Cluster.LogLevel.DEBUG);
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

        log("I have the latest version", Cluster.LogLevel.DEBUG);
      } else {
        // everything matches, break ties using node id
        proposedCoordinatorID = Math.max(proposedCoordinatorID, this.id);
        proposedCoord = proposedCoordinatorID == this.id ? getSelf() : proposedCoord;
        log("Tie breaker: " + proposedCoordinatorID, Cluster.LogLevel.DEBUG);
      }

      log("Updated proposed coordinator: " + proposedCoordinatorID + " message coordinator " + electionMessage.proposedCoordinatorID + " next hop " + nextHop, Cluster.LogLevel.DEBUG);
      
      nextHop = sendElectionMsg(new ElectionMessage(update, proposedCoord, proposedCoordinatorID, electionMessage.activeReplicas), nextHop);

      if(voteTimeout != null){
        voteTimeout.cancel();
      }

      voteTimeout = setTimeout(VOTE_TIMEOUT, new ElectionTimeout(nextHop, electionMessage.activeReplicas));
    }
  }

  /**
   * Handles the receipt of a sync message within the cluster.
   * This method updates the node's history, cancels timeouts, and processes any pending messages.
   *
   * @param sm the {@code SyncMessage} received, containing the update history.
   */
  public void onSyncMessageReceipt(SyncMessage sm){
    this.updateHistory = new HashMap<>(sm.updateHistory);
    deferringMessages = false;
     
    renewHeartbeatTimeout();

    if(electionTimeout != null){
      log("Cancelling election timeout", Cluster.LogLevel.INFO);
      electionTimeout.cancel();
      setElectionTimeout = false;
    }

    if(voteTimeout != null){
      log("Cancelling vote timeout", Cluster.LogLevel.INFO);
      voteTimeout.cancel();
    }
    
    if(pendingMsg.size() > 0){
      log("Solve pending messages", Cluster.LogLevel.INFO);
      // Retry pending messages from current epoch
      for(WriteDataMsg msg : pendingMsg){
        coordinator.tell(new PendingWriteMsg(msg.value, msg.sender), getSelf());
      }
    } else {
      log("No pending messages", Cluster.LogLevel.INFO);
      coordinator.tell(new FlushMsg(), getSelf());
    }
  
  }

  /**
   * Handles the receipt of a flush message from a replica.
   * This method updates the state of flushed replicas and can propose a new view if conditions are met.
   *
   * @param msg the {@code FlushMsg} received from a replica.
   */
  public void onFlushMessage(FlushMsg msg) {
    log("Received flush message from replica " + getSender().path().name(), Cluster.LogLevel.INFO);

    if (isCoordinator()) {
      Set<ActorRef> participants = proposedView.get(epochSeqNumPair.currentEpoch);
      flushedReplicas.add(getSender());
      log("Flushed replicas: " + flushedReplicas.toString(), Cluster.LogLevel.DEBUG);
      log("Can propose view: " + (flushedReplicas.size() >= participants.size()), Cluster.LogLevel.DEBUG);
      if (flushedReplicas.size() >= participants.size()) {
        multicast(new ViewChangeMsg(new EpochSeqNum(epochSeqNumPair.currentEpoch + 1, 0), participants, coordinator));
        currentRequest = 0;
        log("View change message sent", Cluster.LogLevel.INFO);
      }
    }
  }

  /**
   * Handles the receipt of a view change message within the cluster.
   * This method updates the current view and coordinator based on the new view proposed.
   *
   * @param msg the {@code ViewChangeMsg} received, containing the new epoch sequence number and proposed view.
   */
  public void onViewChange(ViewChangeMsg msg) {
    currentView.clear();
    epochSeqNumPair = msg.esn;

    for(ActorRef participant : msg.proposedView){
      currentView.add(participant);
    }

    renewHeartbeatTimeout();

    log( "Participants in the new view: " + currentView.toString(), Cluster.LogLevel.INFO);
    log( "Coordinator in the new view: " + coordinator.path().name(), Cluster.LogLevel.INFO);
    getContext().become(createReceive());
    
    if (isCoordinator()) {
      supervisor.tell(new Client.SetCoordinator(), getSelf());
    }
  }

  /**
   * Handles the receipt of an election acknowledgment message.
   * This method cancels the vote timeout and updates the state to indicate that the next hop has not timed out.
   *
   * @param ack the {@code ElectionAck} received, indicating acknowledgment of the election message.
   */
  public void onElectionAck(ElectionAck ack) {
    voteTimeout.cancel();
    // eventually this will be set to false (assumption that there is always at least a quorum
    nextHopTimedOut = false;
  }

  /**
   * Handles the receipt of a pending write message.
   * If the current node is the coordinator, it requests an update for the pending write message.
   *
   * @param msg the {@code PendingWriteMsg} received, containing the value to be updated.
   */
  public void onPendingWriteMessage(PendingWriteMsg msg) {
    if (isCoordinator()) {
      requestUpdate(msg.value, getSender());
    }
  }


  /**
   * Handles the election timeout message.
   * If the next hop has timed out, this method tries with the subsequent node.
   *
   * @param msg the {@code ElectionTimeout} received, containing the next hop and active replicas.
   */
  public void onElectionTimeout(ElectionTimeout msg) {
    nextHopTimedOut = true;
    // because nextHop timedout we try with the one after that: nextHop+1;
    nextHop = sendElectionMsg(new ElectionMessage(updateHistory,getSelf(),this.id, msg.activeReplicas),msg.next);
    // set timeout with nextHop+1
    voteTimeout = setTimeout(VOTE_TIMEOUT, new ElectionTimeout(nextHop, msg.activeReplicas));
    log("Election timeout. Trying with next node: " + nextHop, Cluster.LogLevel.INFO);
  }

  /**
   * Handles the restart election message.
   * This method initiates the process to restart the election.
   *
   * @param msg the {@code RestartElection} received, indicating the need to restart the election.
   */
  public void onRestartElection(RestartElection msg) {
    log("Restarting election", Cluster.LogLevel.INFO);
    setElectionTimeout = false;
    nextHop = -1;
    multicast(new ElectionRestated());
    startElection();
  }

  /**
   * Handles the election restarted message.
   * This method updates the state to reflect the restarted election and initiates the election process again.
   *
   * @param msg the {@code ElectionRestated} received, indicating the election has been restarted.
   */
  public void onElectionRestarted(ElectionRestated msg) {
    log("Election restarted", Cluster.LogLevel.INFO);
    
    if(electionTimeout != null){
      log("Cancelling election timeout", Cluster.LogLevel.INFO);
      electionTimeout.cancel();
    }
  }

  /**
   * Starts the election process.
   * This method initiates an election by sending an election message to the next hop and setting the necessary timeouts.
   */
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

    createElectionTimeout();
  }

  /**
   * Sends an election message to the appropriate replica.
   * This method sorts the list of participants, finds the next replica whose ID is greater than the current node's ID,
   * and attempts to send the election message. If a timeout occurs, it updates the next hop.
   *
   * @param electionMessage the {@code ElectionMessage} to be sent.
   * @param next the index of the next hop in the list of participants.
   * @return the index of the next hop after sending the election message.
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

  /**
   * Updates the coordinator to the specified actor.
   *
   * @param coordinator the new coordinator {@code ActorRef}.
   */
  private void updateCoordinator(ActorRef coordinator){
    this.coordinator = coordinator;
  }

  /**
   * Checks if the received election message has been updated compared to the current state.
   *
   * @param msg the {@code ElectionMessage} to be checked.
   * @return {@code true} if the message has been updated, {@code false} otherwise.
   */
  private boolean hasBeenUpdated(ElectionMessage msg){
    return !msg.updateHistory.equals(this.updateHistory) || msg.proposedCoordinatorID != this.proposedCoordinatorID;
  }

  /**
   * Handles the receipt of a read history message.
   * This method logs the current update history.
   *
   * @param msg the {@code ReadHistory} message received.
   */
  public void onReadHistory(ReadHistory msg) {
    log("History: " + updateHistory.toString(), Cluster.LogLevel.INFO);
  }

  /**
   * Cleans up the state related to the election process.
   * This method resets the proposed coordinator and coordinator ID.
   */
  private void cleanUp(){
    this.proposedCoord = null;
    this.proposedCoordinatorID = -1;
  }

  /**
   * Checks if the current node is the coordinator.
   *
   * @return {@code true} if the current node is the coordinator, {@code false} otherwise.
   */
  private boolean isCoordinator() {
    if (this.coordinator == null) {
      logger.error("Coordinator is not set");
      return false;
    }
    return coordinator.equals(getSelf());
  }

  /**
   * Creates an election timeout.
   * This method sets an election timeout, prioritizing lower node IDs to trigger the election first.
   * The timeout is staggered based on the node's ID to ensure orderly election initiation.
   */
  private void createElectionTimeout(){
    if(!setElectionTimeout){
      // Stagger election timeouts pioritizing lower nodes to trigger election first
      int time = ELECTION_TIMEOUT + (id * 750);
      log("Setting election timeout to trigger in " + time + "ms", Cluster.LogLevel.INFO);
      electionTimeout = setTimeout(time, new RestartElection());
      setElectionTimeout = true;
    }
  }

}
