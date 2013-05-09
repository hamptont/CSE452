//Hampton Terry - hterry
//Jacob Sanders - jacobs22

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import edu.washington.cs.cse490h.lib.*;

import java.io.*;
import java.lang.reflect.Type;
import java.util.*;
import java.util.Map.Entry;

public class TwitterNode extends RIONode {
	public static double getFailureRate() { return 0/100.0; }
	public static double getRecoveryRate() { return 0/100.0; }
	public static double getDropRate() { return 0/100.0; }
	public static double getDelayRate() { return 0/100.0; }

	private Map<Long, Boolean> acked;
	private byte[] msg;

	private long seq_num;
	private Queue<String> pending_commands;
    private Queue<String> active_commands;
	private int commandInProgress;

	private String username;
	private Map<String, String> tweets;

	private Gson gson;
	private String transaction_id;

    private Map<String, TransactionState> transactionStateMap;


    private static final String TWEET_FILE_SUFFIX = "-tweets";
	private static final String FOLLOWERS_FILE_SUFFIX = "-following";
	private static final String INFO_FILE_SUFFIX = "-info";
	private static final String RECOVERY_FILENAME = "server_temp";

	private static final String JSON_MSG = "msg";
	private static final String JSON_REQUEST_ID = "request_id";
	private static final String JSON_CURRENT_SEQ_NUM = "current_seq_num";
	private static final String JSON_TRANSACTION_ID = "tid";

	private static final String COMMAND_START_TRANSACTION = "start_transaction";
	private static final String COMMAND_COMMIT_TRANSACTION = "commit_transaction";

	private static final String INVALID_TID = "-1";

	private static final String RPC_START_TXN = "start_transaction";
	private static final String RPC_COMMIT = "commit";
    private static final String RPC_ABORT = "abort";
	private static final String RPC_READ = "read";
	private static final String RPC_APPEND = "append";
	private static final String RPC_DELETE = "delete";
	private static final String RPC_CREATE = "create";

    private enum TransactionState {
        COMMITTED,
        IN_PROGRESS,
        ABORTED
    }

	@Override
	public void onRIOReceive(Integer from, int protocol, byte[] msg) {
		// extract the sequence num from the message, update this node's seq_num
		String json = packetBytesToString(msg);
		Map<String, String> map = jsonToMap(json);
		long remote_seq_num = Long.parseLong(map.get(JSON_CURRENT_SEQ_NUM));

		// update the sequence number to be larger than any seen previously
		seq_num = Math.max(remote_seq_num, seq_num) + 1;

		//msg from server, client executes code
		if(from == 0) {
			processMessageAsClient(msg);
		}

		//msg from client, server executes code
		if(from == 1){
			processMessageAsServer(msg);
		}
	}

    /*
     * RIOReceive method for server
     */
	private void processMessageAsServer(byte[] msg) {
		String msgJson = packetBytesToString(msg);
		Map<String, String> msgMap = jsonToMap(msgJson);
		String received = msgMap.get(JSON_MSG);
		String request_id = msgMap.get(JSON_REQUEST_ID);

		System.out.println("message received by server: " + msgJson);
		String command = received.split("\\s")[0];
		String filename = "";
		try{
			//All requests should have a filename except transactions
			filename =  received.split("\\s")[1];
		}catch(Exception e){

		}

		String response = "";

		// populate the response we will send
		Map<String, String> response_map = new TreeMap<String, String>();
		response_map.put(JSON_CURRENT_SEQ_NUM, Long.toString(seq_num));
		response_map.put(JSON_REQUEST_ID, request_id);
		response_map.put(JSON_TRANSACTION_ID, msgMap.get(JSON_TRANSACTION_ID));

		PersistentStorageWriter writer = null;
		PersistentStorageInputStream byte_reader = null;

		// execute the requested command
		if(command.equals(RPC_START_TXN)){
			//request to start a transaction
			response_map.put(JSON_TRANSACTION_ID, Long.toString(seq_num));
			response += RPC_START_TXN;
		}else if(command.equals(RPC_CREATE)) {
			response += "okay";
			try{
				boolean append = false;
				writer = super.getWriter(filename, append);
				TreeMap<String, String> fileMap = new TreeMap<String, String>();
				writer.write(mapToJson(fileMap));

			}catch(Exception e){
				e.printStackTrace();
			}

		} else if(command.equals(RPC_APPEND)) {
			try{
				boolean append = false;
				PersistentStorageReader in = super.getReader(filename);
				Map<String, String> fileMap = jsonToMap(in.readLine());
				in.close();

				if(!fileMap.containsKey(request_id)){
					//duplicate request
					writer = super.getWriter(filename, append);
					String tweet = received.substring(command.length() + filename.length() + 2);
					fileMap.put(request_id, tweet);

					//serialize object to json
					String serialized = mapToJson(fileMap);

					System.out.println("writing to file");
					addToLog(filename, serialized);
					writer.write(serialized);
					removeFromLog(filename);
				} else {
					System.out.println("Append not processed, timestamp already exists: " + received);
				}

				//debug
				System.out.println("MAP VALUES (append): ");
				System.out.println(fileMap.values());
			}catch(Exception e){
				System.out.println();
				e.printStackTrace();
			}
			response += "okay";
		} else if(command.equals(RPC_READ)) {
			try{
				response += RPC_READ + " " + filename + " ";
				PersistentStorageReader in = super.getReader(filename);
				Map<String, String> fileMap = jsonToMap(in.readLine());
				in.close();

				String username = filename.split("-")[0];
				for(String s : fileMap.keySet()){
					//return values -- username of people you are following
					response += s + username + " " + fileMap.get(s) + "\n";
				}
				response = response.trim();
			}catch(Exception e){

			}
		} else if(command.equals(RPC_DELETE)){
			//Removing followers from "-followers" file
			//If user is not currently being followed -- does nothing
			//If user is being followed multiple times -- removes all ocurences
			try{
				//read in treemap from file

				PersistentStorageReader in = super.getReader(filename);
				Map<String, String> fileMap = jsonToMap(in.readLine());
				in.close();

				String unfollow_username = received.substring(command.length() + filename.length() + 2);
				if(fileMap.values().contains(unfollow_username)){
					//remove user
					Set<String> keysToRemove = new HashSet<String>();
					for (Map.Entry<String,String> entry : fileMap.entrySet()) {
						String key = entry.getKey();
						String value = entry.getValue();
						if(value.equals(unfollow_username)){
							keysToRemove.add(key);
						}
					}

					//Can't modify map while iterating -- make modifications after
					for(String key : keysToRemove){
						fileMap.remove(key);
					}

					//serialize object
					String serialized = mapToJson(fileMap);
					addToLog(filename, serialized);
					System.out.println("writing to file");
					writer.write(serialized);
					removeFromLog(filename);
				}

				//debug
				System.out.println("MAP VALUES (remove): ");
				System.out.println(fileMap.values());
			}catch(Exception e){
				e.printStackTrace();
			}
			response += "okay";
		} else if(command.equals(RPC_COMMIT)) {
            //TODO send back commit or abort message
        //    response += RPC_COMMIT;
            response += RPC_COMMIT;
        }else{
			response += "unknown command: " + command;
		}

		// close any oper readers/writers
		if(writer != null){
			try{
				writer.close();
			}catch(Exception e){

			}
		}
		if(byte_reader != null){
			try{
				byte_reader.close();
			}catch(Exception e){

			}
		}

		System.out.println("Server sending response: " + response);
		response_map.put(JSON_MSG, response);
		RIOSend(1, Protocol.TWITTER_PKT, mapToJson(response_map).getBytes());
	}

    /*
     *  RIOReceive for client
     */
	private void processMessageAsClient(byte[] msg) {		
		String json = packetBytesToString(msg);
		Map<String, String> map = jsonToMap(json);
		String received = map.get(JSON_MSG);
		String request_id = map.get(JSON_REQUEST_ID);

		System.out.println("message received by client: " + json);

		String command = received.split("\\s")[0];
		//check to see if more RCP calls need to be sent
		if(command.equals(RPC_START_TXN)){
			// we've received our transaction ID
			transaction_id = map.get(JSON_TRANSACTION_ID);
            transactionStateMap.put(transaction_id, TransactionState.IN_PROGRESS);
		}else if(command.equals(RPC_COMMIT)){
			// we've received the confirmation of a transaction
			//transaction_id = INVALID_TID;
            transactionStateMap.put(transaction_id, TransactionState.COMMITTED);
        }else if(command.equals(RPC_ABORT)){
            transactionStateMap.put(transaction_id, TransactionState.ABORTED);

        }else if(command.equals(RPC_READ)){
			//read response
			//check if it is a read of a '-following' file or '-tweets'
			String filename = received.split("\\s")[1];

			if(filename.endsWith(FOLLOWERS_FILE_SUFFIX)){
				//Send more RCP calls to fetch the tweets
				Set<String> usernames = new HashSet<String>();
				for(int i = 2; i < received.split("\\s").length; i++){
					String username = received.split("\\s")[i];
					usernames.add(username);
				}
				Set<Long> outstandingAcks = rcp_read_multiple(usernames, seq_num);

				callback("read_multiple_callback", new String[]{"java.util.Set", "java.util.Set"}, new Object[]{usernames, outstandingAcks});
				commandInProgress++;
				updateSeqNum(outstandingAcks);
			}else if(filename.endsWith(TWEET_FILE_SUFFIX)){
				//Return tweets to user
				String all_tweets = received.substring(received.split("\\s")[0].length() + received.split("\\s")[1].length() + 2);
				for(int i = 0; i < all_tweets.split("\\n").length; i++){
					String tweet = all_tweets.split("\\n")[i];
					if(tweet.length() > 0){
						String id = tweet.split("\\s")[0];
						String tweet_content = tweet.substring(tweet.split("\\s")[0].length() + 1);
						tweets.put(id, tweet_content);
					}
				}
			}
		}

		this.msg = msg;
		acked.put(Long.parseLong(request_id), true);
	}

	@Override
	public void start() {
		logOutput("Starting up...");

		// Generate a user-level synoptic event to indicate that the node started.
		logSynopticEvent("started");

		// initialize local variables
		acked = new HashMap<Long, Boolean>();
		seq_num = System.currentTimeMillis();    //TODO switch to util class
		tweets = new HashMap<String, String>();
		pending_commands = new LinkedList<String>();
		commandInProgress = 0;
		gson = new Gson();
		transaction_id = INVALID_TID;
        transactionStateMap = new TreeMap<String, TransactionState>();
        active_commands = new LinkedList<String>();


        // finish writing files, if necessary
		readRecoveryFileAndApplyChanges();

		// Write empty temp file
		try{
			writeToRecovery(new TreeMap<String,  String>());
		}catch(IOException e){

		}
	}
	
	
	/*
	 * File manipulation methods
	 */
	

	//Read recovery file and write modifications
	private void readRecoveryFileAndApplyChanges() {		
		try {
			Map<String, String> recoveryMap = getRecoveryMap();
			
			for(Entry<String, String> file : recoveryMap.entrySet()) {
				writeFile(file.getKey(), file.getValue());
			}			
		} catch (Exception e) {
			System.out.println("Unable to recover from log");
			e.printStackTrace();
		}
	}

	// write the json-encoded string of contents to the specified file
	private void writeFile(String filename, String contents) throws IOException {
		PersistentStorageWriter byte_writer = super.getWriter(filename, false);
		byte_writer.write(contents);
		byte_writer.close();
	}
	
	// write the map to the specified file
	private void writeFile(String filename, Map<String, String> contents) throws IOException {
		String jsonMap = mapToJson(contents);
		writeFile(filename, jsonMap);
	}

	// turns the Map into the equivalent json
	private String mapToJson(Map<String, String> map) {
		Type listType = new TypeToken<Map<String, String>>() {}.getType();
		return gson.toJson(map, listType);
	}

	// turns the json into a Map
	private Map<String, String> jsonToMap(String json){
		Type listType = new TypeToken<Map<String, String>>() {}.getType();
		return gson.fromJson(json, listType);
	}

	// return the contents of the recovery file as a map (filename -> json-encoded contents)
	private Map<String, String> getRecoveryMap() throws IOException, ClassNotFoundException {
		//read recovery file
		PersistentStorageReader in = super.getReader(RECOVERY_FILENAME);
		Map<String, String> recoveryMap = jsonToMap(in.readLine());
		in.close();
		return recoveryMap;
	}

	// write the map (filename to json-encoded contents) to the recovery file
	private void writeToRecovery(Map<String, String> recoveryMap) throws IOException {
		writeFile(RECOVERY_FILENAME, recoveryMap);
	}

	//Add file to log
	private void addToLog(String fileName, String json){
		System.out.println("write " + fileName + " to log");
		try{
			//read recovery file
			Map<String, String> recoveryMap = getRecoveryMap();

			//modify recovery file
			recoveryMap.put(fileName, json);

			//write to file
			writeToRecovery(recoveryMap);
		}catch(Exception e){
			System.out.println("ERROR - Could not write recovery file.");
			e.printStackTrace();
		}
	}

	//Remove file from log
	private void removeFromLog(String fileName){
		System.out.println("remove " + fileName + " from log");
		try{
			//read recovery file
			Map<String, String> recoveryMap = getRecoveryMap();

			//modify recovery file
			recoveryMap.remove(fileName);

			//write to file
			writeToRecovery(recoveryMap);
		}catch(Exception e){
			System.out.println("ERROR - Could not write recovery file.");
			e.printStackTrace();
		}
	}
	
	
	/*
	 * COMMAND METHODS
	 * methods used to accept commands from the simulator
	 */


	@Override
	public void onCommand(String command){
		if(pending_commands.isEmpty()){
			// start a command tick if necessary
			callback("commandTickCallback", new String[0], new Object[0]);
		}

		// queue up the command
		//TODO move these around? how do we handle abors by the server?
	//	pending_commands.add(COMMAND_START_TRANSACTION);
		pending_commands.add(command);
	//	pending_commands.add(COMMAND_COMMIT_TRANSACTION);
	}


	/*
         Twitter requirements from project 1 write up:
         Create a user
         Login/logout as user
         Post to a twitter stream
         Add/delete a follower to/from a twitter stream
         Read all (unread) posts that a user is following

        Server = 0
		Client = 1
	 */
	private void onCommand_ordered(String command) {
		String[] split = command.split("\\s");
		String operation = split[0];
		String parameters = null;
		try {
			parameters = command.substring(operation.length() + 1);

		}catch(Exception e){
			//no parameters
		}

		Set<Long> outstandingAcks = new TreeSet<Long>();

		if (operation.equals("create")) {
			if(parameters == null) {
				// no username specified
				System.out.println("No username specified. Unable to create account.");
			} else {
				outstandingAcks = rcp_create(parameters, seq_num);
				callback("create_account_callback", new String[]{"java.lang.String", "java.util.Set"}, new Object[]{parameters, outstandingAcks});
				commandInProgress++;
			}
		} else if(operation.equals("login")) {
			if(parameters == null) {
				// no username specified
				System.out.println("No username specified. Unable to login.");
			} else if(username != null){
				//someone else already logged in
				System.out.print("Account " + username + " is currently logged in.");
				System.out.println("Please log out before you can log in with another account.");
			}else {
				username = parameters;
				outstandingAcks = rcp_login(parameters, seq_num);
				callback("login_callback", new String[]{"java.lang.String", "java.util.Set"}, new Object[]{parameters, outstandingAcks});
				commandInProgress++;
			}
		} else if(operation.equals("logout")) {
			if(parameters == null) {
				// no username specified
				System.out.println("No username specified. Unable to logout.");
			} else {
				username = null;
				tweets.clear();
			}
		} else if(operation.equals("post")) {
			if(parameters == null) {
				// no tweet message
				System.out.println("No message specified. Unable to post tweet.");
			} else if (username == null){
				System.out.println("You are not logged in. Please log in to post messages.");
			} else {
				outstandingAcks = rcp_post(parameters, seq_num);
				callback("post_callback", new String[]{"java.lang.String", "java.util.Set"}, new Object[]{parameters, outstandingAcks});
				commandInProgress++;
			}
		} else if(operation.equals("add")) {
			if(parameters == null) {
				// no username to follow
				System.out.println("No username specified. Unable to follow user.");
			} else {
				outstandingAcks = rcp_add(parameters, seq_num);
				callback("add_callback", new String[]{"java.lang.String", "java.util.Set"}, new Object[]{parameters, outstandingAcks});
				commandInProgress++;
			}
		} else if(operation.equals("delete")) {
			if(parameters == null) {
				// no username to unfollow
				System.out.println("No username specified. Unable to unfollow user.");
			} else {
				outstandingAcks = rcp_delete(parameters, seq_num);
				callback("delete_callback", new String[]{"java.lang.String", "java.util.Set"}, new Object[]{parameters, outstandingAcks});
				commandInProgress++;
			}
		} else if(operation.equals("read")) {
			if(parameters != null) {
				System.out.println("No parameters should be provided for the read command.");
			} else {
				outstandingAcks = rcp_read(parameters, seq_num);
				callback("read_callback", new String[]{"java.lang.String", "java.util.Set"}, new Object[]{parameters, outstandingAcks});
				commandInProgress++;
			}
        } else if (operation.equals(COMMAND_START_TRANSACTION)){
            outstandingAcks = rcp_start_transaction(seq_num);
            callback("start_transaction_callback", new String[]{"java.util.Set"}, new Object[]{outstandingAcks});
            commandInProgress++;

        } else if (operation.equals(COMMAND_COMMIT_TRANSACTION)){
            outstandingAcks = rcp_commit_transaction(seq_num);
            callback("commit_transaction_callback", new String[]{"java.util.Set"}, new Object[]{outstandingAcks});
            commandInProgress++;

        } else {
			System.out.println("Unknown operation: " + operation);
		}
		updateSeqNum(outstandingAcks);
	}
	
	
	/*
	 * RPC CALLS
	 * methods used to send RPCs to the server
	 */
	
    /*
	private void startTransaction(){
		Set<Long> outstandingAcks = new TreeSet<Long>();
		outstandingAcks = rcp_transaction(seq_num);
		callback("transaction_callback", new String[]{"java.util.Set"}, new Object[]{outstandingAcks});
		commandInProgress++;

		updateSeqNum(outstandingAcks);
	}
	*/

	private void rpc_call(int node, int p, String msg, long seq_num){
		System.out.println("rcp_call sending message: " + node + " " + seq_num + " " + msg);
		Map<String, String> json_map = new TreeMap<String, String>();
		json_map.put(JSON_CURRENT_SEQ_NUM, Long.toString(seq_num));
		json_map.put(JSON_MSG, msg);
		json_map.put(JSON_REQUEST_ID, Long.toString(seq_num));
		json_map.put(JSON_TRANSACTION_ID, transaction_id);

		String json = mapToJson(json_map);
		RIOSend(node, Protocol.TWITTER_PKT, Utility.stringToByteArray(json));
		System.out.println("done sending");
	}

	private void callback(String methodName, String[] paramTypes, Object[] params) {
		int timeout = 10;
		//Make tick callbacks fire every tick
		if(methodName.equals("commandTickCallback")){
			timeout = 1;
		}
		try {
			Callback cb = new Callback(Callback.getMethod(methodName, this, paramTypes),
					this, params);
			addTimeout(cb, timeout);
		} catch (NoSuchMethodException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
	}

	private Set<Long> rcp_create(String parameters, long seq_num){
		rpc_call(0, Protocol.TWITTER_PKT, RPC_CREATE + " " + parameters + TWEET_FILE_SUFFIX, seq_num);
		rpc_call(0, Protocol.TWITTER_PKT, RPC_CREATE + " " + parameters + FOLLOWERS_FILE_SUFFIX, seq_num + 1);
		rpc_call(0, Protocol.TWITTER_PKT, RPC_CREATE + " " + parameters + INFO_FILE_SUFFIX, seq_num + 2);
		Set<Long> returned = new TreeSet<Long>();
		returned.add(seq_num);
		returned.add(seq_num + 1);
		returned.add(seq_num + 2);
		acked.put(seq_num, false);
		acked.put(seq_num + 1, false);
		acked.put(seq_num + 2, false);
		return returned;
	}

	private Set<Long> rcp_login(String parameters, long seq_num){
		Set<Long> returned = new TreeSet<Long>();
		rpc_call(0, Protocol.TWITTER_PKT, RPC_READ + " " + parameters + INFO_FILE_SUFFIX, seq_num);
		acked.put(seq_num, false);
		returned.add(seq_num);
		return returned;
	}

	private Set<Long> rcp_post(String parameters, long seq_num){
		Set<Long> returned = new TreeSet<Long>();
		rpc_call(0, Protocol.TWITTER_PKT, RPC_APPEND +" " + username + TWEET_FILE_SUFFIX + " " + parameters, seq_num);
		acked.put(seq_num, false);
		returned.add(seq_num);
		return returned;
	}

	private Set<Long> rcp_add(String parameters, long seq_num){
		Set<Long> returned = new TreeSet<Long>();
		rpc_call(0, Protocol.TWITTER_PKT, RPC_APPEND + " " + username + FOLLOWERS_FILE_SUFFIX + " " + parameters, seq_num);
		acked.put(seq_num, false);
		returned.add(seq_num);
		return returned;
	}

	private Set<Long> rcp_delete(String parameters, long seq_num){
		Set<Long> returned = new TreeSet<Long>();
		rpc_call(0, Protocol.TWITTER_PKT, RPC_DELETE + " " + username + FOLLOWERS_FILE_SUFFIX + " " + parameters, seq_num);
		acked.put(seq_num, false);
		returned.add(seq_num);
		return returned;
	}

	private Set<Long> rcp_read(String parameters, long seq_num){
		Set<Long> returned = new TreeSet<Long>();
		rpc_call(0, Protocol.TWITTER_PKT, RPC_READ + " " + username + FOLLOWERS_FILE_SUFFIX, seq_num);
		acked.put(seq_num, false);
		returned.add(seq_num);
		return returned;
	}

	//sends multiple RCP read requests to get tweets from all users being followed
	private Set<Long> rcp_read_multiple(Set<String> usernames, long seq_num){
		Set<Long> returned = new TreeSet<Long>();
		int count = 0;
		for(String username: usernames){
			rpc_call(0, Protocol.TWITTER_PKT, RPC_READ + " " + username + TWEET_FILE_SUFFIX, seq_num + count);
			returned.add(seq_num + count);
			acked.put(seq_num + count, false);
		}

		return returned;
	}

    /*
	private Set<Long> rcp_transaction(long seq_num){
		Set<Long> returned = new TreeSet<Long>();
		rpc_call(0, Protocol.TWITTER_PKT, RPC_START_TXN, seq_num);
		acked.put(seq_num, false);
		returned.add(seq_num);
		return returned;
	} */

    private Set<Long> rcp_start_transaction(long seq_num){
        Set<Long> returned = new TreeSet<Long>();
        rpc_call(0, Protocol.TWITTER_PKT, RPC_START_TXN, seq_num);
        acked.put(seq_num, false);
        returned.add(seq_num);
        return returned;
    }

    private Set<Long> rcp_commit_transaction(long seq_num){
        Set<Long> returned = new TreeSet<Long>();
        rpc_call(0, Protocol.TWITTER_PKT, RPC_COMMIT, seq_num);
        acked.put(seq_num, false);
        returned.add(seq_num);
        return returned;
    }


	/*
	 * CALLBACKS
	 * 
	 * Used to achieve asynchronous operation demanded by the simulator
	 */
	
	
	public void commandTickCallback(){

        TransactionState state = transactionStateMap.get(transaction_id);
        if(commandInProgress == 0 && !pending_commands.isEmpty()){
            /*
            if(transaction_id == INVALID_TID || state == TransactionState.COMMITTED){
                if(state == TransactionState.COMMITTED){
                    active_commands.clear();
                    pending_commands.remove();
                }
                if(!pending_commands.isEmpty()){
                    active_commands.add(COMMAND_START_TRANSACTION);
                    active_commands.add(pending_commands.peek());
                    active_commands.add(COMMAND_COMMIT_TRANSACTION);
                }
            }else if(state == TransactionState.IN_PROGRESS){
                //wait
            }else if(state == TransactionState.ABORTED){
                transaction_id = INVALID_TID;
                active_commands.clear();
                active_commands.add(COMMAND_START_TRANSACTION);
                active_commands.add(pending_commands.peek());
                active_commands.add(COMMAND_COMMIT_TRANSACTION);
            }
                */
            if(state == TransactionState.COMMITTED){
                pending_commands.remove();
                transaction_id = INVALID_TID;
            }else if(state == TransactionState.ABORTED){
                transaction_id = INVALID_TID;
                active_commands.clear();
            }

            if(transaction_id == INVALID_TID){
                active_commands.clear();
                if(!pending_commands.isEmpty()){
                    active_commands.add(COMMAND_START_TRANSACTION);
                    active_commands.add(pending_commands.peek());
                    active_commands.add(COMMAND_COMMIT_TRANSACTION);
                }
            }
        }
		if(commandInProgress == 0 && !active_commands.isEmpty()){
            String command = active_commands.remove();
            System.out.println("executing command: " + command);
            onCommand_ordered(command);
		}

		if(!pending_commands.isEmpty()) {
			callback("commandTickCallback", new String[0], new Object[0]);
		}
	}

	public void login_callback(String parameters, Set<Long> outstandingAcks) {
		System.out.println("login_callback called: " + parameters);
		boolean all_acked = allAcked(outstandingAcks);

		if(all_acked) {
			for(Long ack : outstandingAcks) {
				acked.remove(ack);
			}
			commandInProgress--;
		} else {
			// retry
			long min_ack = Long.MAX_VALUE;
			for(Long num : outstandingAcks){
				min_ack = Math.min(num, min_ack);
			}
			outstandingAcks = rcp_login(parameters, min_ack);
			callback("login_callback", new String[]{"java.lang.String", "java.util.Set"}, new Object[]{parameters, outstandingAcks});
		}
	}

	public void delete_callback(String parameters, Set<Long> outstandingAcks) {
		System.out.println("delete_callback called: " + parameters);
		boolean all_acked = allAcked(outstandingAcks);

		if(all_acked) {
			for(Long ack : outstandingAcks) {
				acked.remove(ack);
			}
			commandInProgress--;
		} else {
			// retry
			long min_ack = Long.MAX_VALUE;
			for(Long num : outstandingAcks){
				min_ack = Math.min(num, min_ack);
			}
			rcp_delete(parameters, min_ack);
			callback("delete_callback", new String[]{"java.lang.String", "java.util.Set"}, new Object[]{parameters, outstandingAcks});
		}
	}

	public void post_callback(String parameters, Set<Long> outstandingAcks) {
		System.out.println("post_callback called: " + parameters);
		boolean all_acked = allAcked(outstandingAcks);

		if(all_acked) {
			for(Long ack : outstandingAcks) {
				acked.remove(ack);
			}
			commandInProgress--;
		} else {
			// retry
			long min_ack = Long.MAX_VALUE;
			for(Long num : outstandingAcks){
				min_ack = Math.min(num, min_ack);
			}
			rcp_post(parameters, min_ack);
			callback("post_callback", new String[]{"java.lang.String", "java.util.Set"}, new Object[]{parameters, outstandingAcks});
		}
	}

	public void add_callback(String parameters, Set<Long> outstandingAcks) {
		System.out.println("add_callback called: " + parameters);
		boolean all_acked = allAcked(outstandingAcks);

		if(all_acked) {
			for(Long ack : outstandingAcks) {
				acked.remove(ack);
			}
			commandInProgress--;
		} else {
			// retry
			long min_ack = Long.MAX_VALUE;
			for(Long num : outstandingAcks){
				min_ack = Math.min(num, min_ack);
			}
			rcp_add(parameters, min_ack);
			callback("add_callback", new String[]{"java.lang.String", "java.util.Set"}, new Object[]{parameters, outstandingAcks});
		}
	}

	public void read_callback(String parameters, Set<Long> outstandingAcks) {
		System.out.println("read_callback called");
		boolean all_acked = allAcked(outstandingAcks);

		if(all_acked) {
			for(Long ack : outstandingAcks) {
				acked.remove(ack);
			}
			commandInProgress--;
		} else {
			// retry
			long min_ack = Long.MAX_VALUE;
			for(Long num : outstandingAcks){
				min_ack = Math.min(num, min_ack);
			}
			rcp_read(parameters, min_ack);
			callback("read_callback", new String[]{"java.lang.String", "java.util.Set"}, new Object[]{parameters, outstandingAcks});
		}
	}

	public void read_multiple_callback(Set<String> parameters, Set<Long> outstandingAcks){
		System.out.println("read_multiple_callback called");
		boolean all_acked = allAcked(outstandingAcks);
		if(all_acked) {
			for(Long ack : outstandingAcks) {
				acked.remove(ack);
			}
			System.out.println((username + "'s follower's tweets:").toUpperCase());
			for(String val : tweets.keySet()){
				System.out.println( val.substring(13) + " : " + tweets.get(val));
			}
			commandInProgress--;
		} else {
			long min_ack = Long.MAX_VALUE;
			for(Long num : outstandingAcks){
				min_ack = Math.min(num, min_ack);
			}
			rcp_read_multiple(parameters, min_ack);
			callback("read_multiple_callback", new String[]{"java.util.Set", "java.util.Set"}, new Object[]{parameters, outstandingAcks});
		}
	}

	public void create_account_callback(String parameters, Set<Long> outstandingAcks) {
		System.out.println("create_account_callback called: " + parameters);
		boolean all_acked = allAcked(outstandingAcks);

		if(all_acked) {
			for(Long ack : outstandingAcks) {
				acked.remove(ack);
			}
			String response = packetBytesToString(this.msg);

			System.out.println("Account created!");
			System.out.println("response: " + response);

			commandInProgress--;
		} else {
			long min_ack = Long.MAX_VALUE;
			for(Long num : outstandingAcks){
				min_ack = Math.min(num, min_ack);
			}
			rcp_create(parameters, min_ack);
			callback("create_account_callback", new String[]{"java.lang.String", "java.util.Set"}, new Object[]{parameters, outstandingAcks});
		}
	}

    /*
	public void transaction_callback(Set<Long> outstandingAcks) {
		System.out.println("transaction callback called");

		boolean all_acked = allAcked(outstandingAcks);
		if(all_acked) {
			for(Long ack : outstandingAcks) {
				acked.remove(ack);
			}
			commandInProgress--;
		} else {
			long min_ack = Long.MAX_VALUE;
			for(Long num : outstandingAcks){
				min_ack = Math.min(num, min_ack);
			}
			rcp_transaction(min_ack);
			callback("transaction_callback", new String[]{"java.util.Set"}, new Object[]{outstandingAcks});
		}
	}
	*/

    public void start_transaction_callback(Set<Long> outstandingAcks) {
        System.out.println("start_transaction_callback called: " );
        boolean all_acked = allAcked(outstandingAcks);

        if(all_acked) {
            for(Long ack : outstandingAcks) {
                acked.remove(ack);
            }
            String response = packetBytesToString(this.msg);

            System.out.println("Transaction started");
            System.out.println("response: " + response);

            commandInProgress--;
        } else {
            long min_ack = Long.MAX_VALUE;
            for(Long num : outstandingAcks){
                min_ack = Math.min(num, min_ack);
            }
            rcp_start_transaction(min_ack);
            callback("start_transaction_callback", new String[]{"java.util.Set"}, new Object[]{outstandingAcks});
        }
    }


    public void commit_transaction_callback(Set<Long> outstandingAcks) {
        System.out.println("commit_transaction_callback called: " );
        boolean all_acked = allAcked(outstandingAcks);

        if(all_acked) {
            for(Long ack : outstandingAcks) {
                acked.remove(ack);
            }
            String response = packetBytesToString(this.msg);

            System.out.println("Transaction committed");
            System.out.println("response: " + response);

            commandInProgress--;
        } else {
            long min_ack = Long.MAX_VALUE;
            for(Long num : outstandingAcks){
                min_ack = Math.min(num, min_ack);
            }
            rcp_commit_transaction(min_ack);
            callback("commit_transaction_callback", new String[]{"java.util.Set"}, new Object[]{outstandingAcks});
        }
    }


/*
     * ASSORED HELPER FUNCTIONS
     */

    /*
     * Increments commandInProgress by one
     * If from 0 to 1, starts a new transaction
     */
    /*
    private void incrementCommandInProgress(){
        if(commandInProgress == 0 ){
            commandInProgress++;

        }
    }
    */

    /*
    * Decrements commandInProgress by one
    * If from 1 to 0, commits transaction
    */
    /*
    private void decrementCommandInProgress(){

    }
    */

	/*
	 * returns true if all longs in outstandingAcks are
	 * present in acked with the value 'true'
	 */
	private boolean allAcked(Set<Long> outstandingAcks) {
		boolean all_acked = true;
		for(Long i : outstandingAcks) {
			if(!acked.get(i)){
				all_acked = false;
			}
		}
		//debug
		if(all_acked){
			System.out.println("ALL MESSAGES ACKED!");
		}
		return all_acked;
	}
	
	/*
	 * makes seq_num the max of seq_num and all of outstandingAcks, +1
	 */
	private void updateSeqNum(Set<Long> outstandingAcks){
		//Find the max of the outstanding acks sent
		for(Long val : outstandingAcks){
			seq_num = Math.max(seq_num, val);
		}
		//Next unused sequence number
		seq_num++;
	}

	@Override
	public String packetBytesToString(byte[] bytes) {
		RIOPacket packet = RIOPacket.unpack(bytes);
		if (packet == null) {
			return super.packetBytesToString(bytes);
		}
		return packet.toString();
	}

	public void logError(String output) {
		log(output, System.err);
	}

	public void logOutput(String output) {
		log(output, System.out);
	}

	public void log(String output, PrintStream stream) {
		stream.println("Node " + addr + ": " + output);
	}

	@Override
	public String toString() {
		return super.toString();
	}



}