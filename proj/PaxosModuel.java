import java.io.IOException;
import java.lang.reflect.Type;
import java.util.*;

import com.google.gson.reflect.TypeToken;
import edu.washington.cs.cse490h.lib.PersistentStorageWriter;
import edu.washington.cs.cse490h.lib.Utility;

public class PaxosModuel {
	private static final String EMPTY_VALUE = "";
	private static final String PAXOS_STATE_FILENAME = "paxosStateFile";
	private static Type TypeSetInt = new TypeToken<Set<Integer>>() {}.getType();
	private static Type TypeLong = new TypeToken<Long>() {}.getType();
	private static Type TypeMapLongAcceptorState = new TypeToken<Map<Long, AcceptorState>>() {}.getType();
	private static Type TypeMapLongString = new TypeToken<Map<Long, String>>() {}.getType();
	private static Type TypeMapLongUpdateRequest = new TypeToken<Map<Long, UpdateRequest>>() {}.getType();


	private Set<Integer> knownPaxosNodes;

	private long currentProposalNumber;

	// acceptor
	private class AcceptorState {
		long highestPromised;
		long highestAccepted;
		String value = null;
	}
	private Map<Long, AcceptorState> stateOfRound;

	// learner
	private TreeMap<Long, String> roundToTransaction;

	// proposer
	/**
	 * Represents the values associated with a promise
	 *
	 */
	private class Promise {
		long proposalNum;
		String value;

		Promise(long proposalNum, String value){
			this.proposalNum = proposalNum;
			this.value = value;
		}
	}

	/**
	 * describes the state that a proposer needs to keep
	 */
	private class UpdateRequest {
		int requestingServerId;
		String requestedValue;
		Set<Integer> participants;
		Map<Integer, Promise> promised;
		Set<Integer> accepted;
		Set<Integer> learned;
		long proposalNum;
	}

	private Map<Long, UpdateRequest> roundToUpdateRequest;

	//prepare
	public class PrepareResponse {
		long n;
		String value;
	}

	//private PersistentStorageWriter byte_writer; //~~~~
	private TwitterNode encompassingNode;


	public PaxosModuel(TwitterNode encompassingNode){
		//TODO do all the recovery/initial file creation shit
		//	try {
		this.encompassingNode = encompassingNode;
		//    byte_writer = encompassingNode.getPersistentStorageWriter(PAXOS_STATE_FILENAME, false);  //~~~~

		//encompassingNode.getReader(PAXOS_STATE_FILENAME);
		//	} catch (IOException e) {
		knownPaxosNodes = new TreeSet<Integer>();
		//currentRoundOfVoting = 0;
		currentProposalNumber = Utility.getRNG().nextLong();


		//voteInProgress = false;
		// acceptor
		stateOfRound = new TreeMap<Long, AcceptorState>();
		// learner
		roundToTransaction = new TreeMap<Long, String>();
		// proposer
		roundToUpdateRequest = new TreeMap<Long, UpdateRequest>();
		//	}
	}

	/**
	 * Returns the proposalNumber to be used for the given round, 
	 * or -1 if a new round cannot be started
	 * @return
	 */
	// proposer
	public long startNewVote(int requestingNodeId, long round, String value) {
		if((stateOfRound.get(round) == null 
				&& roundToTransaction.get(round) == null
				&& roundToUpdateRequest.get(round) == null)) {

			// if the current round is not in progress
			UpdateRequest newUpdate = new UpdateRequest();
			roundToUpdateRequest.put(round, newUpdate);
			newUpdate.requestingServerId = requestingNodeId;
			newUpdate.proposalNum = currentProposalNumber;
			newUpdate.participants = new TreeSet<Integer>(knownPaxosNodes);
			newUpdate.requestedValue = value;

			return currentProposalNumber++;
			//TODO implement check for identical request that has already been issued
		} else {
			return -1L;  
		}    	  	
	}

	/**
	 * If paxos node has promised or accepted another request: return the previous proposal number and previous value in 
	 * a prepareResponse 
	 * If paxos can accept request, return prepareResponse with null value. Paxos node promises to ignore all future
	 * with smaller n values
	 * If paxos needs to reject request, return null
	 * @param round
	 * @param n
	 * @return
	 */
	// acceptor
	public PrepareResponse prepare(long round, long n){
		PrepareResponse response = new PrepareResponse();

		AcceptorState state = stateOfRound.get(round);
		if(state == null){
			state = new AcceptorState();
			state.highestPromised = n;
			state.highestAccepted = Long.MIN_VALUE;
			state.value = EMPTY_VALUE;
			stateOfRound.put(round, state);
			response.n = n;
			response.value = EMPTY_VALUE;
			return response;
		}

		if(n < Math.max(state.highestAccepted, state.highestPromised)){
			return null;
		}

		response.n = state.highestAccepted;
		response.value = state.value;
		state.highestPromised = n;

		saveStateToDisk();

		return response;
	}

	/**
	 * Return true for accept 
	 * Return false for reject
	 * @param round
	 * @param n
	 * @param value
	 * @return
	 */
	// acceptor
	public boolean propose(long round, long n, String value){

		if(value == null) {
			throw new IllegalArgumentException("value cannot be null");
		}
		AcceptorState state = stateOfRound.get(round);

		if(state != null && state.highestPromised > n){
			return false;
		}

		if(state == null){
			state = new AcceptorState();
			stateOfRound.put(round, state);
		}

		//Check to see if we have already accepted a different value
		if(state.value != null && !state.value.equals(value)){
			return false;
		}

		state.value = value;
		state.highestAccepted = n;

		saveStateToDisk();

		return true;
	}

	/**
	 * Returns true after request is applied to disk
	 * @param round
	 * @param value
	 * @return
	 */
	// learner
	public boolean learn(long round, String value){    	
		roundToTransaction.put(round, value);

		saveStateToDisk();

		return true;
	}

	/**
	 * Mark that the given node has learned the value for a given round
	 * @param round
	 * @param learnedNode
	 */
	public void learned(long round, int learnedNode) {
		UpdateRequest request = roundToUpdateRequest.get(round);
		if(request.learned == null){
			request.learned = new TreeSet<Integer>();
		}

		request.learned.add(learnedNode);

		saveStateToDisk();
	}

	/**
	 * Return true if a majority of nodes have promised
	 * @param round
	 * @param promiseProposalNum
	 * @param node
	 * @return
	 */
	public boolean promise(long round, long promiseProposalNum, String promiseValue, int node){
		UpdateRequest request = roundToUpdateRequest.get(round);
		if(request.promised == null){
			request.promised = new TreeMap<Integer, Promise>();
		}

		Promise promise = new Promise(promiseProposalNum, promiseValue);
		request.promised.put(node, promise);


		boolean majorityPromised;
		if(request.promised.keySet().size() > request.participants.size() / 2){
			long highestProposalNumWithNonEmptyValue = -1L;
			for(Map.Entry<Integer, Promise> entry : request.promised.entrySet()) {
				if(!entry.getValue().value.equals(EMPTY_VALUE)) {
					if (entry.getValue().proposalNum > highestProposalNumWithNonEmptyValue){
						highestProposalNumWithNonEmptyValue = entry.getValue().proposalNum;
						request.requestedValue = entry.getValue().value;
					}                	
				}
			}
			majorityPromised = true;
		} else {
			majorityPromised = false;
		}        

		//TODO should this be here or above??
		saveStateToDisk();

		return majorityPromised;
	}

	public String getProposedValue(long round) {
		UpdateRequest updateReq = roundToUpdateRequest.get(round);
		return updateReq == null ? null : updateReq.requestedValue;
	}

	public long getProposalNumForRound(long round){
		UpdateRequest updateReq = roundToUpdateRequest.get(round);
		return updateReq == null ? -1L : updateReq.proposalNum;
	}

	/*
	public int getProposingNodeId(long round){
		UpdateRequest updateReq = roundToUpdateRequest.get(round);
		return updateReq == null ? null : updateReq.requestingServerId;
	}
*/
	
	//learner
	public String getLearnedValue(long round) {
		return roundToTransaction.get(round);
	}

	/**
	 * Returns all learned values starting at the specified round
	 * @param round
	 * @return
	 */
	// learner
	public Map<Long, String> getAllLearnedValues(long round){
		SortedMap<Long, String> relevantRounds = roundToTransaction.tailMap(round);
		return relevantRounds;
	}

	/**
	 * If a majority have accepted, return true, else false
	 * @param round
	 * @param node
	 * @return
	 */
	//proposer
	public boolean accepted(long round, int node){
		UpdateRequest request = roundToUpdateRequest.get(round);

		request.accepted.add(node);

		saveStateToDisk();

		return request.accepted.size() > knownPaxosNodes.size()/2;
	}

//	/**
//	 * Adds the given node to the paxos group
//	 * @param node
//	 */
//	public void addToPaxosPool(int node) {
//		knownPaxosNodes.add(node);
//	}
//
//	/**
//	 * Removes the given node from the paxos group
//	 * @param node
//	 */
//	public void removeFromPaxosPool(int node) {
//		knownPaxosNodes.remove(node);
//	}
	
	/**
	 * Sets the nodes that are acting as the current paxos group
	 * @param paxosNodes
	 */
	public void setPaxosGroup(Set<Integer> paxosNodes) {
		knownPaxosNodes = new TreeSet<Integer>(paxosNodes);
	}

	/**
	 * Returns the set of currently known nodes in the paxos group
	 * @return
	 */
	public Set<Integer> getPaxosGroup(){
		return Collections.unmodifiableSet(knownPaxosNodes);
	}

	public PrepareResponse getNewPrepareResponse(){
		return new PrepareResponse();
	}

	/*
        Saves the paxox node's private variables to disk in json form
	 */
	public void saveStateToDisk(){
		Map<String, String> contents = new TreeMap<String, String>();

		contents.put("nodesInPaxos", TwitterNode.mapToJson(knownPaxosNodes, TypeSetInt));
		contents.put("currentProposalNumber", TwitterNode.mapToJson(currentProposalNumber, TypeLong));
		contents.put("stateOfRound", TwitterNode.mapToJson(stateOfRound, TypeMapLongAcceptorState));
		contents.put("roundToTransaction", TwitterNode.mapToJson(roundToTransaction, TypeMapLongString));
		contents.put("roundToUpdateRequest", TwitterNode.mapToJson(roundToUpdateRequest, TypeMapLongUpdateRequest));

		Type TypeMapStringString = new TypeToken<Map<String, String>>() {}.getType();
		String json = TwitterNode.mapToJson(contents, TypeMapStringString);
		System.out.println("JSON saved to disk: " + json);

		try{
			encompassingNode.writeFile(PAXOS_STATE_FILENAME, json);
		}catch(IOException e){
			System.out.println("Error: unable to write paxos recovery file");
		}
	}

	/*
        Loads paxos node's private variables from disk.
        Returns true if successful, false otherwise (no valid file exists)
	 */
	public boolean recoverStateFromDisk(){
		try{
			Map<String, String> file = encompassingNode.readJsonFile(PAXOS_STATE_FILENAME);
			System.out.println("JSON read from disk: " + file.toString());

			this.knownPaxosNodes = (Set<Integer>)TwitterNode.jsonToMap(file.get("nodesInPaxos"), TypeSetInt);
			this.currentProposalNumber = (Long)TwitterNode.jsonToMap(file.get("currentProposalNumber"), TypeLong);
			this.stateOfRound = (Map<Long, AcceptorState>)TwitterNode.jsonToMap(file.get("stateOfRound"), TypeMapLongAcceptorState);
			this.roundToTransaction = (TreeMap<Long, String>)TwitterNode.jsonToMap(file.get("roundToTransaction"), TypeMapLongString);
			this.roundToUpdateRequest = (Map<Long, UpdateRequest>)TwitterNode.jsonToMap(file.get("roundToUpdateRequest"), TypeMapLongUpdateRequest);

			return true;
		}catch(Exception e){
			System.out.println("Error: unable to write paxos recovery file");
			return false;
		}
	}
}
