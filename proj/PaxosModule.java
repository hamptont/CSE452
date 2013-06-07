import java.io.IOException;
import java.lang.reflect.Type;
import java.util.*;

import com.google.gson.reflect.TypeToken;
import edu.washington.cs.cse490h.lib.Utility;

/**
 * A class to maintain the state associated Paxos.
 * Enables proposer/learner/accepter to be on the same machine.
 * Enabled multiple 'rounds' for concurrent rounds of Paxos.
 *
 */
public class PaxosModule {
	private static final String EMPTY_VALUE = "";
	private static final String PAXOS_STATE_FILENAME = "paxosStateFile";
	private static final Type SET_INT_TYPE = new TypeToken<Set<Integer>>() {}.getType();
	private static final Type LONG_TYPE = new TypeToken<Long>() {}.getType();
	private static final Type MAP_LONG_ACCEPTORSTATE_TYPE = new TypeToken<TreeMap<Long, AcceptorState>>() {}.getType();
	private static final Type MAP_LONG_STRING_TYPE = new TypeToken<TreeMap<Long, String>>() {}.getType();
	private static final Type MAP_LONG_UPDATE_REQUEST_TYPE = new TypeToken<TreeMap<Long, UpdateRequest>>() {}.getType();

	private long paxosGroupVersion;
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
		//int requestingServerId;
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

	public PaxosModule(TwitterNode encompassingNode){
		this.encompassingNode = encompassingNode;// for reader/writer
		
		paxosGroupVersion = -2L;
		knownPaxosNodes = new TreeSet<Integer>();
		currentProposalNumber = Utility.getRNG().nextLong();


		//voteInProgress = false;
		// acceptor
		stateOfRound = new TreeMap<Long, AcceptorState>();
		// learner
		roundToTransaction = new TreeMap<Long, String>();
		// proposer
		roundToUpdateRequest = new TreeMap<Long, UpdateRequest>();

		
		recoverStateFromDisk();
		saveStateToDisk();
	}

	/**
	 * Returns the proposalNumber to be used for the given round, 
	 * or -1 if a new round cannot be started
	 * @return
	 */
	// proposer
	public long startNewVote(long round, String value) {
		if((stateOfRound.get(round) == null 
				&& roundToTransaction.get(round) == null
				&& roundToUpdateRequest.get(round) == null)) {

			// if the current round is not in progress
			UpdateRequest newUpdate = new UpdateRequest();
			roundToUpdateRequest.put(round, newUpdate);
			//newUpdate.requestingServerId = requestingNodeId;
			newUpdate.proposalNum = currentProposalNumber;
			newUpdate.participants = new TreeSet<Integer>(knownPaxosNodes);
			newUpdate.requestedValue = value;

			return currentProposalNumber++;
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

		if(state == null){
			state = new AcceptorState();
			state.value = EMPTY_VALUE;
			state.highestAccepted = Long.MIN_VALUE;
			state.highestPromised = Long.MIN_VALUE;

			stateOfRound.put(round, state);
		} else if (state.highestPromised > n) {
			// if we're already promised higher
			return false;
		} else if(!state.value.equals(EMPTY_VALUE) && !state.value.equals(value)){
			//Check to see if we have already accepted a different value
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
		if(request == null) {
			request = new UpdateRequest();
			roundToUpdateRequest.put(round, request);
		}
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

		if(request.accepted == null) {
			request.accepted = new TreeSet<Integer>();
		}

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
	 * Sets the nodes that are acting as the current paxos group, 
	 * if the version of the proposed group is larger than the version of the current,
	 * or if this is the first group proposed
	 * Saves the state to disk.
	 * @param paxosNodes
	 * @param version
	 */
	public void setPaxosGroup(Set<Integer> paxosNodes, long version) {
		if(version > paxosGroupVersion) {
			knownPaxosNodes = new TreeSet<Integer>(paxosNodes);
			paxosGroupVersion = version;
			saveStateToDisk();
		}
	}

	/**
	 * Returns an unmodifiable reference to the set of nodes in paxos
	 * @return
	 */
	public Set<Integer> getPaxosGroup(){
		return Collections.unmodifiableSet(knownPaxosNodes);
	}
	
	public long getPaxosGroupVersion() {
		return paxosGroupVersion;
	}
	
	public long getHighestLearnedRound() {
		if(roundToTransaction.isEmpty()) {
			return -1L;
		}
		return roundToTransaction.lastKey();
	}

	/*
	public PrepareResponse getNewPrepareResponse(){
		return new PrepareResponse();
	}
*/
	/*
        Saves the paxox node's private variables to disk in json form
	 */
	public void saveStateToDisk(){
		Map<String, String> contents = new TreeMap<String, String>();

		contents.put("nodesInPaxos", TwitterNode.objectToJson(knownPaxosNodes, SET_INT_TYPE));
		contents.put("currentProposalNumber", TwitterNode.objectToJson(currentProposalNumber, LONG_TYPE));
		contents.put("stateOfRound", TwitterNode.objectToJson(stateOfRound, MAP_LONG_ACCEPTORSTATE_TYPE));
		contents.put("roundToTransaction", TwitterNode.objectToJson(roundToTransaction, MAP_LONG_STRING_TYPE));
		contents.put("roundToUpdateRequest", TwitterNode.objectToJson(roundToUpdateRequest, MAP_LONG_UPDATE_REQUEST_TYPE));
		contents.put("paxosGroupVersion", Long.toString(paxosGroupVersion));

		Type TypeMapStringString = new TypeToken<Map<String, String>>() {}.getType();
		String json = TwitterNode.objectToJson(contents, TypeMapStringString);
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

			this.knownPaxosNodes = (Set<Integer>)TwitterNode.jsonToObject(file.get("nodesInPaxos"), SET_INT_TYPE);
			this.currentProposalNumber = (Long)TwitterNode.jsonToObject(file.get("currentProposalNumber"), LONG_TYPE);
			this.stateOfRound = (Map<Long, AcceptorState>)TwitterNode.jsonToObject(file.get("stateOfRound"), MAP_LONG_ACCEPTORSTATE_TYPE);
			this.roundToTransaction = (TreeMap<Long, String>)TwitterNode.jsonToObject(file.get("roundToTransaction"), MAP_LONG_STRING_TYPE);
			this.roundToUpdateRequest = (Map<Long, UpdateRequest>)TwitterNode.jsonToObject(file.get("roundToUpdateRequest"), MAP_LONG_UPDATE_REQUEST_TYPE);
			this.paxosGroupVersion = Long.parseLong(file.get("paxosGroupVersion"));
			return true;
		}catch(Exception e){
			System.out.println("Error: unable to write paxos recovery file");
			return false;
		}
	}
}
