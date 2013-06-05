import java.io.IOException;
import java.lang.reflect.Type;
import java.util.*;

import com.google.gson.reflect.TypeToken;
import edu.washington.cs.cse490h.lib.PersistentStorageWriter;
import edu.washington.cs.cse490h.lib.Utility;

public class PaxosModuel {
	private static final String PAXOS_STATE_FILENAME = "paxosStateFile";
    private static Type TypeSetInt = new TypeToken<Set<Integer>>() {}.getType();
    private static Type TypeLong = new TypeToken<Long>() {}.getType();
    private static Type TypeMapLongAcceptorState = new TypeToken<Map<Long, AcceptorState>>() {}.getType();
    private static Type TypeMapLongString = new TypeToken<Map<Long, String>>() {}.getType();
    private static Type TypeMapLongUpdateRequest = new TypeToken<Map<Long, UpdateRequest>>() {}.getType();


    private Set<Integer> nodesInPaxos;

    //private long currentRoundOfVoting;
    private long currentProposalNumber;

    //private boolean voteInProgress;

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
    /*
    private enum ProposerState {
        PREPARE, PROPOSE, SENDING_TO_LEARNER
    }
*/
    private class Promise {
    	long proposalNum;
    	String value;
    	
    	Promise(long proposalNum, String value){
    		this.proposalNum = proposalNum;
    		this.value = value;
    	}
    }
    private class UpdateRequest {
        int requestingServerId;
        String requestedValue;
//      ProposerState currentState;
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
    
    // start a transaction proposal
    /*
    public class RoundInfo {
    	long roundNum;
    	long proposalNum;
    }
    */

    private PersistentStorageWriter byte_writer; //~~~~
    TwitterNode encompassingNode;


    public PaxosModuel(TwitterNode encompassingNode){
    	//TODO do all the recovery/initial file creation shit
    //	try {
            this.encompassingNode = encompassingNode;
        //    byte_writer = encompassingNode.getPersistentStorageWriter(PAXOS_STATE_FILENAME, false);  //~~~~

			//encompassingNode.getReader(PAXOS_STATE_FILENAME);
	//	} catch (IOException e) {
			nodesInPaxos = new TreeSet<Integer>();
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
    		newUpdate.participants = new TreeSet<Integer>(nodesInPaxos);
    		newUpdate.requestedValue = value;
    		
    		//RoundInfo returned = new RoundInfo();
        	//returned.proposalNum = currentProposalNumber++; //increment after we're done assigning it
        	//returned.roundNum = round;
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
            stateOfRound.put(round, state);
            response.n = n;
            response.value = null;
            return response;
        }

        if(n < Math.max(state.highestAccepted, state.highestPromised)){
            return null;
        }

        response.n = state.highestAccepted;
        response.value = state.value;
        state.highestPromised = n;

        //TODO save paxos state to disk

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

        //TODO save paxos state to disk
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
    	//currentRoundOfVoting = Math.max(currentRoundOfVoting, round + 1);
    	
        //write to disk
        //ack

        //TODO save paxos state to disk
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
    }

    /**
     * Return true if a majority of nodes have promised
     * @param round
     * @param responseN
     * @param node
     * @return
     */
    public boolean promise(long round, long responseN, String responseValue, int node){
        UpdateRequest request = roundToUpdateRequest.get(round);
        if(request.promised == null){
            request.promised = new TreeMap<Integer, Promise>();
        }

        Promise promise = new Promise(responseN, responseValue);
        request.promised.put(node, promise);

        //TODO save paxos state to disk
        if(request.promised.keySet().size() > request.participants.size() / 2){
            long highestProposalNumWithNonNullValue = -1L;
            for(Map.Entry<Integer, Promise> entry : request.promised.entrySet()) {
                if(entry.getValue().value != null) {
                	if (entry.getValue().proposalNum > highestProposalNumWithNonNullValue){
                		highestProposalNumWithNonNullValue = entry.getValue().proposalNum;
                		request.requestedValue = entry.getValue().value;
                	}                	
                }
            }
            return true;
        }
        return false;
    }
    
    public String getProposedValue(long round) {
    	UpdateRequest updateReq = roundToUpdateRequest.get(round);
    	return updateReq == null ? null : updateReq.requestedValue;
    }
    
    public long getProposalNumForRound(long round){
    	UpdateRequest updateReq = roundToUpdateRequest.get(round);
    	return updateReq == null ? -1L : updateReq.proposalNum;
    }
    
    public int getProposingNodeId(long round){
    	UpdateRequest updateReq = roundToUpdateRequest.get(round);
    	return updateReq == null ? null : updateReq.requestingServerId;
    }
    
    public String getLearnedValue(long round) {
    	return roundToTransaction.get(round);
    }
    
    /**
     * Returns all learned values starting at the specified round
     * @param round
     * @return
     */
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
    public boolean accepted(long round, int node){
        UpdateRequest request = roundToUpdateRequest.get(round);

        request.accepted.add(node);

        return request.accepted.size() > nodesInPaxos.size()/2;
    }

    /**
     * Adds the given node to the paxos group
     * @param node
     */
    public void addToPaxosGroup(int node) {
        nodesInPaxos.add(node);
    }

    /**
     * Removes the given node from the paxos group
     * @param node
     */
    public void removeFromPaxosGroup(int node) {
        nodesInPaxos.remove(node);
    }

    public Set<Integer> getPaxosNodes(){
        return Collections.unmodifiableSet(nodesInPaxos);
    }
    
    public PrepareResponse getNewPrepareResponse(){
    	return new PrepareResponse();
    }

    /*
        Saves the paxox node's private variables to disk in json form
     */
    public void saveStateToDisk(){
        Map<String, String> contents = new TreeMap<String, String>();

        contents.put("nodesInPaxos", TwitterNode.mapToJson(nodesInPaxos, TypeSetInt));
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

            this.nodesInPaxos = (Set<Integer>)TwitterNode.jsonToMap(file.get("nodesInPaxos"), TypeSetInt);
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
