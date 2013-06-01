import java.util.*;

public class PaxosModuel {

    private Set<Long> nodesInPaxos;

    private long currentRoundOfVoting;
    private long currentProposalNumber;

    private boolean voteInProgress;

    // acceptor
    private class AcceptorState {
        long highestPromised;
        long highestAccepted;
        String value = null;
    }

    private Map<Long, AcceptorState> stateOfRound;

    // learner
    private Map<Long, String> roundToTransaction;

    // proposer
    private enum ProposerState {
        PREPARE, PROPOSE, SENDING_TO_LEARNER
    }

    private class UpdateRequest {
        String requestingServerId;
        String requestedValue;
        ProposerState currentState;
        Set<Long> participants;
        Map<Long, String> promised;
        Set<Long> accepted;
        Set<Long> learned;
        long n;
    }

    private Map<Long, UpdateRequest> roundToUpdateRequest;

    //prepare
    public class PrepareResponse {
        long n;
        String value;
    }

    public PaxosModuel(){
        nodesInPaxos = new TreeSet<Long>();
        currentRoundOfVoting = 0;
        currentProposalNumber = 0;
        voteInProgress = false;
        // acceptor
        stateOfRound = new TreeMap<Long, AcceptorState>();
        // learner
        roundToTransaction = new TreeMap<Long, String>();
        // proposer
        roundToUpdateRequest = new TreeMap<Long, UpdateRequest>();
    }

    /*
      If paxos node has promised or accepted another request: return the previous proposal number and previous value in
           a prepareResponse
      If paxos can accept request, return prepareResponse with null value. Paxos node promises to ignore all future
           with smaller n values
      If paxos needs to reject request, return null
     */
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

    /*
        Return true for accept
        Return false for reject
     */
    public boolean propose(long round, long n, String value){

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

    /*
        Returns true after request is applied to disk
     */
    public boolean learn(long round, String value){
        //write to disk
        //ack

        //TODO save paxos state to disk
        return true;
    }

    /*
        Return true if a majority of nodes have responded with null or the proposed value
     */
    public boolean promise(long round, PrepareResponse response, long node){
        UpdateRequest request = roundToUpdateRequest.get(round);
        if(request.promised == null){
            request.promised = new TreeMap<Long, String>();
        }

        if(response != null){
            request.promised.put(node, response.value);
        }

        //TODO save paxos state to disk
        if(request.promised.keySet().size() > nodesInPaxos.size() / 2){
            int counter = 0;
            for(Map.Entry<Long, String> entry : request.promised.entrySet()) {
                if(entry.getValue() == null || entry.getValue().equals(request.requestedValue)) {
                    counter++;
                }
            }
            return counter > nodesInPaxos.size() / 2;
        }
        return false;
    }

    /*
        If a majority have accepted, return true, else false
     */
    public boolean accepted(long round, long node){
        UpdateRequest request = roundToUpdateRequest.get(round);

        request.accepted.add(node);

        return request.accepted.size() > nodesInPaxos.size()/2;
    }

    public void addToPaxosGroup(long node) {
        nodesInPaxos.add(node);
    }

    public void removeFromPaxosGroup(long node) {
        nodesInPaxos.remove(node);
    }

    public Set<Long> getPaxosNodes(){
        return Collections.unmodifiableSet(nodesInPaxos);
    }
}
