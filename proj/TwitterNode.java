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

	private Map<Long, Boolean> acked; // requestId -> acked?
	private byte[] msg; //received message

	private long seq_num;
	private Queue<String> pending_commands;
	private Queue<String> active_commands;
	private int commandsInProgress;

	private String username;
	private Map<String, String> tweets;
	private Set<Integer> servers;

	private static Gson gson;
	private String transaction_id;

	private Map<String, TransactionState> transactionStateMap;

	//TODO are we storing all of this in file?
	private Map<Integer, TransactionData> clientMap;
	private long currentTransactionRound;
	private Set<Integer> paxosNodes;

	private String role;


	/**
	 * String constants
	 */

	//private static final int NUM_SERVER_NODES = 128;

	private static final String TWEET_FILE_SUFFIX = "-tweets";
	private static final String FOLLOWERS_FILE_SUFFIX = "-following";
	private static final String INFO_FILE_SUFFIX = "-info";
	private static final String RECOVERY_FILENAME = "server_temp";
    private static final String STORED_TRANSACTIONS_FILENAME = "stored_transactions";

	private static final String TWEET_TIMESTAMP_TOKEN = "~";

	private static final String JSON_MSG = "msg";
	private static final String JSON_REQUEST_ID = "request_id";
	private static final String JSON_CURRENT_SEQ_NUM = "current_seq_num";
	private static final String JSON_TRANSACTION_ID = "tid";

	private static final String COMMAND_START_TRANSACTION = "start_transaction";
	private static final String COMMAND_COMMIT_TRANSACTION = "commit_transaction";
	
	// usage: "n joinPaxosGroup m" where n should request to join the paxos group that m is a part of
	private static final String COMMAND_JOIN_PAXOS = "joinPaxosGroup";
	private static final String REMOVE_FROM_PAXOS_GROUP = "removeFromPaxos";//TODO implement this
	
	// usage: "n usePaxosGroup m" where n is a server which should use m as its paxos entry point
	private static final String COMMAND_USE_PAXOS = "usePaxosGroup";
	
	// usage: "n useServer m" where n is a client and m is a server that the client should send requests to
	private static final String COMMAND_USE_SERVER = "useServer";//TODO implement

	private static final String INVALID_TID = "-1";

	// RPC calls for client-server interaction
	private static final String RPC_START_TXN = "start_transaction";
	private static final String RPC_COMMIT = "commit";
	private static final String RPC_ABORT = "abort";
	private static final String RPC_READ = "read";
	private static final String RPC_APPEND = "append";
	private static final String RPC_DELETE = "delete";
	private static final String RPC_CREATE = "create";
	private static final String STATUS_SUCCESS = "success";

	// role assignment commands
	// example command usage: "n assign server"
	private static final String ASSIGN_ROLE_COMMAND = "assign";
	private static final String SERVER_NODE_ROLE = "server";
	private static final String PAXOS_NODE_ROLE = "paxos";
	private static final String CLIENT_NODE_ROLE = "client";


	/**
	 * Private helper classes
	 */

	private enum TransactionState {
		COMMITTED,
		IN_PROGRESS,
		ABORTED
	}   

	private class TransactionData {
		public String tid;
		public String rid;
		public long proposedRound;
		public Map<String, String> rid_action_map;
        public int client_id;
	}

	private class TwitterFile {
		public String fileVersion;
		public Map<String, String> contents;
	}

	@Override
	public void start() {
		logOutput("Starting up...");

		// Generate a user-level synoptic event to indicate that the node started.
		logSynopticEvent("started");

		// initialize local variables
		acked = new HashMap<Long, Boolean>();
		seq_num = 0L;
		tweets = new HashMap<String, String>();
		servers = new TreeSet<Integer>();
		pending_commands = new LinkedList<String>();
		commandsInProgress = 0;
		gson = new Gson();
		transaction_id = INVALID_TID;
		transactionStateMap = new TreeMap<String, TransactionState>();
		active_commands = new LinkedList<String>();
		clientMap = new TreeMap<Integer, TransactionData>();
		currentTransactionRound = 0L;
		knownServers = new TreeSet<Integer>();
		paxosNodes = new TreeSet<Integer>();

		// paxos module initialization
		pax = new PaxosModuel(this);

		// finish writing files, if necessary

		readRecoveryFileAndApplyChanges();

		// Write empty temp file
		try{
			writeToRecovery(new TreeMap<String,  String>());
		}catch(IOException e){

		}
	}

	@Override
	public void onRIOReceive(Integer from, int protocol, byte[] msg) {
		// extract the sequence num from the message, update this node's seq_num
		String json = packetBytesToString(msg);
		Type mapType = new TypeToken<Map<String, String>>() {}.getType();
		Map<String, String> map = (Map<String, String>)jsonToObject(json, mapType);


		long remote_seq_num = Long.parseLong(map.get(JSON_CURRENT_SEQ_NUM));

		// update the sequence number to be larger than any seen previously
		seq_num = Math.max(remote_seq_num, seq_num) + 1;


		if(role.equals(CLIENT_NODE_ROLE)) {
			//msg from server, client executes code
			processMessageAsClient(msg);
		} else if(role.equals(SERVER_NODE_ROLE) && protocol == Protocol.TWITTER_PKT){
			//msg from client, server executes code
			processClientMessageAsServer(msg, from);
		} else if (role.equals(SERVER_NODE_ROLE) && protocol == Protocol.PAXOS_PKT) {
			processPaxosMessageAsServer(msg,from);
		} else if(role.equals(PAXOS_NODE_ROLE)) {
			processMessageAsPaxos(msg, from);
		} else {
			throw new IllegalStateException("Invalid node role: "+ role);
		}		
	}

	/*
	 * RIOReceive method for server
	 */
	private void processMessageAsServer(byte[] msg, int client_id) {
		String msgJson = packetBytesToString(msg);
		Type mapType = new TypeToken<Map<String, String>>() {}.getType();
		Map<String, String> msgMap = (Map<String, String>)jsonToObject(msgJson, mapType);
		String received = msgMap.get(JSON_MSG);
		String request_id = msgMap.get(JSON_REQUEST_ID);

		System.out.println("message received by server: " + msgJson);
		String command = received.split("\\s")[0];

		String response = "";

		// populate the response we will send
		Map<String, String> response_map = new TreeMap<String, String>();
		response_map.put(JSON_CURRENT_SEQ_NUM, Long.toString(seq_num));
		response_map.put(JSON_REQUEST_ID, request_id);
		response_map.put(JSON_TRANSACTION_ID, msgMap.get(JSON_TRANSACTION_ID));

		// execute the requested command
		if(!transactionStateMap.containsKey(msgMap.get(JSON_TRANSACTION_ID)) 
				&& !command.equals(RPC_START_TXN)) {
			//received request from unknown transaction
			//send abort, client must retry
			response = RPC_ABORT;
		} else if(command.equals(RPC_START_TXN)){
			//request to start a transaction
			TransactionData transaction = clientMap.get(client_id);
			if(transaction != null && transaction.rid.equals(request_id)){
				//transaction already started -- duplicate message
				//return transaction ID of current transaction
				response_map.put(JSON_TRANSACTION_ID, transaction.tid);
                response = RPC_START_TXN;

            } else {
                //Check if transaction has already been committed and saved to disk
                boolean transaction_finished = false;
                String client_transaction_identifier = client_id + TWEET_TIMESTAMP_TOKEN + msgMap.get(JSON_TRANSACTION_ID);
                try{
                    PersistentStorageReader in = super.getReader(STORED_TRANSACTIONS_FILENAME);
                    String fileContents = in.readLine();
                    in.close();
                    Map<String, String> processedTransactions = jsonToTwitfile(fileContents).contents;
                    if(processedTransactions.containsKey(client_transaction_identifier)){
                        transaction_finished = true;
                    }
                }catch(IOException e){

                }
                if(transaction_finished){
                    response = RPC_COMMIT;

                } else {
                    //new transaction
                    //set up transaction start
                    response_map.put(JSON_TRANSACTION_ID, Long.toString(currentTransactionRound));
                    transaction = new TransactionData();
                    transaction.tid = Long.toString(currentTransactionRound);
                    transaction.rid = request_id;
                    transaction.rid_action_map = new TreeMap<String, String>();
                    transaction.proposedRound = currentTransactionRound;
                    transaction.client_id = client_id;
                    clientMap.put(client_id, transaction);
                    transactionStateMap.put(transaction.tid, TransactionState.IN_PROGRESS);

                    response = RPC_START_TXN;
                }
            }
		} else if(command.equals(RPC_COMMIT)) {
			//request to commit transaction
			TransactionData transaction = clientMap.get(client_id);
			if(transaction == null){
				//no active transaction
				//need to start transaction
				response = RPC_ABORT;
			} else {
				Map<String, String> requests =  transaction.rid_action_map;

				boolean abort = txnMustAbort(client_id);

				if(abort){
					response = RPC_ABORT;
					clientMap.remove(client_id);
				}else{
					response = RPC_COMMIT;
					//process the actual commands


                    //Check to see if transaction changes have already been applied to disk
                   // boolean apply_changes = true;
                    //If transaction has already been applied to disk (duplicate transaction request), respond to client with success
                    Map<String, String> processedTransactions = null;
                    String client_transaction_identifier = client_id + TWEET_TIMESTAMP_TOKEN + msgMap.get(JSON_TRANSACTION_ID);
                    try{
                        PersistentStorageReader in = super.getReader(STORED_TRANSACTIONS_FILENAME);
                        String fileContents = in.readLine();
                        in.close();
                        processedTransactions = jsonToTwitfile(fileContents).contents;
                        if(processedTransactions.containsKey(client_transaction_identifier)){
                           // apply_changes = false;
                            response_map.put(JSON_MSG, response);
                            System.out.println("Server sending response: " + response_map);
                            RIOSend(client_id, Protocol.TWITTER_PKT, objectToJson(response_map, mapType).getBytes());
                            return;
                        }
                    }catch(IOException e){

                    }

                    //If new Transaction - send to PAXOS


                    response_map.put(JSON_COMMAND, RPC_PAX_STORE_VALUE_REQUEST);
                    response_map.put(JSON_PAX_ROUND, Long.toString(currentTransactionRound));
                    Map<String, String> paxos_value = new TreeMap<String, String>();
                    paxos_value.put("HELLO", "WORLD"); //TODO put value in here
                    response_map.put(JSON_PAX_VALUE, objectToJson(paxos_value, mapType));
                    int paxos_node_to_send_to = Integer.MIN_VALUE;
                    for(Integer paxos_node : paxosNodes){
                        paxos_node_to_send_to = Math.max(paxos_node_to_send_to, paxos_node);
                    }

                    System.out.println("Server sending message to paxos: " + response_map);
                    RIOSend(paxos_node_to_send_to, Protocol.TWITTER_PKT, objectToJson(response_map, mapType).getBytes());


                    /*
                    //If transaction has not already been applied to disk (not duplicate request)
                    if(apply_changes){
                        Map<String, String> writeAheadLog = new TreeMap<String, String>();

                        //Add STORED_TRANSACTIONS_FILENAME to write ahead log
                        if(processedTransactions == null){
                            processedTransactions = new TreeMap<String, String>();
                        }
                        processedTransactions.put(client_transaction_identifier, "");

                        TwitterFile twitFile = new TwitterFile();
                        twitFile.fileVersion = Long.toString(currentTransactionRound);
                        twitFile.contents = processedTransactions;
                        writeAheadLog.put(STORED_TRANSACTIONS_FILENAME, twitfileToJson(twitFile));

                        //Add client requests to write ahead log
                        for(String s : requests.keySet()){
                            String json = requests.get(s);
                            Map<String, String> jsonMap = (Map<String, String>)jsonToObject(json, mapType);
                            processTransaction(jsonMap, writeAheadLog);
                            System.out.println("TRANSACTION BEING ADDED TO WRITE AHEAD LOG: " + json);
                        }

                        //write transaction modifications to write ahead log
                        try{
                            writeFile(RECOVERY_FILENAME, writeAheadLog);
                        }catch(IOException e){

                        }
                    }

                    response_map.put(JSON_MSG, response);

                    //All transaction modifications have been applied to write ahead log
                    //Okay to send commit success to client
                    System.out.println("Server sending response: " + response_map);
                    RIOSend(client_id, Protocol.TWITTER_PKT, objectToJson(response_map, mapType).getBytes());

                    //If transaction has not already been applied to disk (not duplicate request)
                    if(apply_changes){
                        //move modified files from write ahead log to disk
                        readRecoveryFileAndApplyChanges();

                        //Write empty temp file to clear out write ahead log
                        try{
                            writeToRecovery(new TreeMap<String,  String>());
                        }catch(IOException e){

                        }
                    }
                    */
					return;
				}
			}
		} else {
			//regular twitter command
			TransactionData transaction = clientMap.get(client_id);
			if(transaction == null){
				//no active transaction
				//need to start transaction
				response = RPC_ABORT;
			} else if(command.equals(RPC_READ)){
				//read request
				//reads currently are processed right away, not stored in the transaction log
				response = processTransaction(msgMap, new HashMap<String, String>());
			}else{
				//write request
				//store request in transaction map to be applied on commit
				transaction.rid_action_map.put(request_id, msgJson);
			}
		}

		//Check to see if we already finished our commit
		if(!response.equals(RPC_COMMIT)){
			boolean abort = txnMustAbort(client_id);
			if(abort){
				response = RPC_ABORT;
			}
		}

		response_map.put(JSON_MSG, response);

		System.out.println("Server sending response: " + response_map);
		RIOSend(client_id, Protocol.TWITTER_PKT, objectToJson(response_map, mapType).getBytes());
	}

	private boolean txnMustAbort(int clientId) {
		TransactionData transaction = clientMap.get(clientId);
		//String txnId = transaction.tid;
		String proposedVersion = Long.toString(transaction.proposedRound);
		Map<String, String> operations = transaction.rid_action_map;

		// to keep track of which files will be wholly deleted/made in this txn
		Set<String> deletedFiles = new TreeSet<String>();
		Set<String> newlyCreatedFiles = new TreeSet<String>();

		// for each operation, 
		// make sure that the file version is consistent with the transaction
		Type mapType = new TypeToken<Map<String, String>>() {}.getType();
		for(Entry<String, String> op : operations.entrySet()) {
			Map<String,String> map = (Map<String,String>)jsonToObject(op.getValue(), mapType);

			String command = map.get(JSON_MSG).split("\\s")[0];
			String filename = map.get(JSON_MSG).split("\\s")[1];

			if(command.equals(RPC_APPEND) || command.equals(RPC_READ)) {
				if(!newlyCreatedFiles.contains(filename)) {
					// if the existing file is the one being used
					try {
						PersistentStorageReader reader = super.getReader(filename);
						String json = reader.readLine();
						TwitterFile file = jsonToTwitfile(json);

						if(proposedVersion.compareTo(file.fileVersion) <= 0) {
							// if the proposedVersion is less than or equal to the file version,
							// the file has been written and we need to abort
							System.out.println("transaction aborted~");
							return true;
						}

					} catch (FileNotFoundException e) {
						// if the file is deleted, txn must abort
						System.out.println("transaction aborted~");
						return true;
					} catch (IOException e) {
						throw new RuntimeException("Could not read file: "+filename,e);
					}
				}
			} else if (command.equals(RPC_CREATE)) {
				if (deletedFiles.contains(filename)) {
					// we deleted the file before, so it's ok to make
					deletedFiles.remove(filename);
					newlyCreatedFiles.add(filename);
				} else {
					try {
						PersistentStorageReader reader = super.getReader(filename);
						String json = reader.readLine();
						TwitterFile file = jsonToTwitfile(json);

						if(json != null && proposedVersion.compareTo(file.fileVersion) < 0) {
							// if the proposedVersion is less than or equal to the file version,
							// the file has been written and we need to abort
							System.out.println("transaction aborted~");
							return true;
						} else {
							newlyCreatedFiles.add(filename);
						}
					} catch (FileNotFoundException e) {
						// file doesn't exist, okay to create file
						newlyCreatedFiles.add(filename);
					} catch (IOException e) {
						throw new RuntimeException("Could not read file: "+filename,e);
					}
				}
			} else if (command.equals(RPC_DELETE)) {
				if(newlyCreatedFiles.contains(filename)) {
					// we created this file, ok to remove
					newlyCreatedFiles.remove(filename);
					deletedFiles.add(filename);
				} else {
					try {
						PersistentStorageReader reader = super.getReader(filename);
						String json = reader.readLine();
						TwitterFile file = jsonToTwitfile(json);

						if(proposedVersion.compareTo(file.fileVersion) < 0) {
							// if the proposedVersion is less than or equal to the file version,
							// the file has been written and we need to abort
							System.out.println("transaction aborted~");
							return true;
						} else {
							deletedFiles.add(filename);
						}
					} catch (FileNotFoundException e) {
						// file doesn't exist anymore, can't delete it!
						System.out.println("transaction aborted~");
						return true;
					} catch (IOException e) {
						throw new RuntimeException("Could not read file: " + filename,e);
					}
				}
			}
		}

		// everything ok!
		return false;
	}

	/*
	 *  RIOReceive for client
	 */
	private void processMessageAsClient(byte[] msg) {		
		String json = packetBytesToString(msg);
		Type mapType = new TypeToken<Map<String, String>>() {}.getType();
		Map<String, String> map = (Map<String, String>)jsonToObject(json, mapType);
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
				commandsInProgress++;
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



	/*
	 * This method actually applies the twitter command to disk.
	 * Should only be called after the transaction has been committed
	 * and all commands in the transaction as been checked to see if they need to be aborted
	 *
	 * Processes exactly one RCP call - should be called multiple tiems for each command in the transaction.
	 */
	private String processTransaction(Map<String, String> msgMap, Map<String, String> writeAheadLog){
		String response = "";

		String received = msgMap.get(JSON_MSG);
		String request_id = msgMap.get(JSON_REQUEST_ID);
		String command = received.split("\\s")[0];

		String filename = "";
		String fileContents = "";
		try{
			//All requests should have a filename except transactions
			//try to read file if it exists
			filename =  received.split("\\s")[1];

			//check to see if file has been modified and stored in write ahead log
			if(writeAheadLog.containsKey(filename)){
				//file has been modified during this transaction -- get modified file from write ahead log
				fileContents = writeAheadLog.get(filename);
			}else{
				//file has not been modified during this transaction -- get file from disk
				PersistentStorageReader in = super.getReader(filename);
				fileContents = in.readLine();
				in.close();
			}
		}catch(Exception e){
			//No filename or file
		}

		if(command.equals(RPC_CREATE)) {
			response += STATUS_SUCCESS;
			try{
				TreeMap<String, String> fileMap = new TreeMap<String, String>();
				TwitterFile twitFile = new TwitterFile();
                twitFile.fileVersion = Long.toString(currentTransactionRound);
                twitFile.contents = fileMap;
				writeAheadLog.put(filename, twitfileToJson(twitFile));

			}catch(Exception e){
				e.printStackTrace();
			}

		} else if(command.equals(RPC_APPEND)) {
			try{
				boolean append = false;
				TwitterFile twitFile = jsonToTwitfile(fileContents);
				Map<String, String> fileMap = twitFile.contents;

				if(!fileMap.containsKey(request_id)){
					//duplicate request

					twitFile = new TwitterFile();
					twitFile.fileVersion = Long.toString(seq_num);
					twitFile.contents = fileMap;

					String tweet = received.substring(command.length() + filename.length() + 2);
					fileMap.put(request_id, tweet);

					//serialize object to json
					String serialized = twitfileToJson(twitFile);

					System.out.println("writing to file");
					addToLog(filename, serialized);
					writeAheadLog.put(filename, serialized);
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
			response += STATUS_SUCCESS;
		} else if(command.equals(RPC_READ)) {
			try{
				response += RPC_READ + " " + filename + " ";
				TwitterFile twitFile = jsonToTwitfile(fileContents);

				Map<String, String> fileMap = twitFile.contents;

				String username = filename.split("-")[0];
				for(String s : fileMap.keySet()){
					//return values -- username of people you are following
					response += s + TWEET_TIMESTAMP_TOKEN + username + " " + fileMap.get(s) + "\n";
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

				Type mapType = new TypeToken<Map<String, String>>() {}.getType();
				Map<String, String> fileMap = (Map<String, String>)jsonToObject(fileContents, mapType);

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
					TwitterFile twitFile = new TwitterFile();
					twitFile.fileVersion = Long.toString(currentTransactionRound);
					twitFile.contents = fileMap;
					String serialized = twitfileToJson(twitFile);
					writeAheadLog.put(filename, serialized);
				}

				//debug
				System.out.println("MAP VALUES (remove): ");
				System.out.println(fileMap.values());
			}catch(Exception e){
				e.printStackTrace();
			}
			response += STATUS_SUCCESS;
		}else{
			response += "unknown command: " + command;
		}

		// close the storage writer, if one was opened
		return response;
	}


	/*
	 * File manipulation methods
	 */


	//Read recovery file and write modifications
	private void readRecoveryFileAndApplyChanges() {		
		try {
			Map<String, String> recoveryMap = readJsonFile(RECOVERY_FILENAME);

			for(Entry<String, String> file : recoveryMap.entrySet()) {
				writeFile(file.getKey(), file.getValue());
			}			
		} catch (Exception e) {
			System.out.println("Unable to recover from log");
		}
	}

	// write the json-encoded string of contents to the specified file
	public void writeFile(String filename, String contents) throws IOException {
		PersistentStorageWriter byte_writer = super.getWriter(filename, false);
		byte_writer.write(contents);
		byte_writer.close();
	}

	// write the map to the specified file
	private void writeFile(String filename, Map<String, String> contents) throws IOException {
		Type mapType = new TypeToken<Map<String, String>>() {}.getType();

		String jsonMap = objectToJson(contents, mapType);
		writeFile(filename, jsonMap);
	}

	// turns the Object into the equivalent json
	public static String objectToJson(Object obj, Type objType) {

		return gson.toJson(obj, objType);
	}

	// turns the json into an Object
	public static Object jsonToObject(String json, Type objType){

		return gson.fromJson(json, objType);
	}


	// turns a TwitterFile into the corresponding json
	private String twitfileToJson(TwitterFile twitfile) {
		return gson.toJson(twitfile);
	}

	// turns json into a twitterFile
	private TwitterFile jsonToTwitfile(String json){
		return gson.fromJson(json, TwitterFile.class);
	}

	// return the contents of the recovery file as a map (filename -> json-encoded contents)

	/**
	 * Reads filename from disk
	 * Assumes file is a json encoded Map<String, String>
	 * Returns Map object
	 * @param filename
	 * @return
	 * @throws IOException
	 * @throws ClassNotFoundException
	 */
	public Map<String, String> readJsonFile(String filename) throws IOException, ClassNotFoundException {
		//read recovery file
		PersistentStorageReader in = super.getReader(filename);
		Type mapType = new TypeToken<Map<String, String>>() {}.getType();
		Map<String, String> recoveryMap = (Map<String, String>)jsonToObject(in.readLine(), mapType);
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
			Map<String, String> recoveryMap = readJsonFile(RECOVERY_FILENAME);

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
			Map<String, String> recoveryMap = readJsonFile(RECOVERY_FILENAME);

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

	private boolean isValidRole(String proposedRole) {
		return CLIENT_NODE_ROLE.equals(proposedRole) || SERVER_NODE_ROLE.equals(proposedRole) || PAXOS_NODE_ROLE.equals(proposedRole);
	}

	@Override
	public void onCommand(String command){		
		String[] split = command.split("\\s");
		String operation = split[0];
		String parameters = null;
		try {
			parameters = command.substring(operation.length() + 1);

		}catch(Exception e){
			//no parameters
		}

		if(operation.equals(ASSIGN_ROLE_COMMAND)) {
			if(isValidRole(split[1])) {
				role = split[1];
			} else {
				System.out.println();
				throw new IllegalArgumentException("Error: is not a valid role: "+ split[1]);
			}
		} else if (operation.equals(COMMAND_JOIN_PAXOS)){
			if(!PAXOS_NODE_ROLE.equals(role)) {
				throw new IllegalStateException("Error: must be assigned as a paxos node to join a paxos group.");
			}
			
			int nodeToAsk = Integer.parseInt(split[1]);
			
			//TODO send message using request RPC_PAX_JOIN_GROUP_REQUEST
			//TODO complete this
			Map<String, String> message = new TreeMap<String, String>();
			message.put(JSON_COMMAND, RPC_PAX_JOIN_GROUP_REQUEST);
		} else if (operation.equals(COMMAND_USE_PAXOS)) {
			if(!SERVER_NODE_ROLE.equals(role)) {
				throw new IllegalStateException("Error: must be assigned as a server node to use a paxos group.");
			}
			
			int nodeToUse = Integer.parseInt(split[1]);
			paxosNodes.add(nodeToUse);
		} else if (operation.equals(COMMAND_USE_SERVER)) {			
			if(!CLIENT_NODE_ROLE.equals(role)) {
				throw new IllegalStateException("Error: must be assigned as a client node to use a server.");
			}
			
			servers.add(Integer.parseInt(split[1]));
		} else {
			if(pending_commands.isEmpty()){
				// start a command tick if necessary
				callback("commandTickCallback", new String[0], new Object[0]);
			}

			// queue up the command
			pending_commands.add(command);
		}

	}


	/*
         Twitter requirements from project 1 write up:
         Create a user
         Login/logout as user
         Post to a twitter stream
         Add/delete a follower to/from a twitter stream
         Read all (unread) posts that a user is following
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
				commandsInProgress++;
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
				commandsInProgress++;
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
				commandsInProgress++;
			}
		} else if(operation.equals("add")) {
			if(parameters == null) {
				// no username to follow
				System.out.println("No username specified. Unable to follow user.");
			} else {
				outstandingAcks = rcp_add(parameters, seq_num);
				callback("add_callback", new String[]{"java.lang.String", "java.util.Set"}, new Object[]{parameters, outstandingAcks});
				commandsInProgress++;
			}
		} else if(operation.equals("delete")) {
			if(parameters == null) {
				// no username to unfollow
				System.out.println("No username specified. Unable to unfollow user.");
			} else {
				outstandingAcks = rcp_delete(parameters, seq_num);
				callback("delete_callback", new String[]{"java.lang.String", "java.util.Set"}, new Object[]{parameters, outstandingAcks});
				commandsInProgress++;
			}
		} else if(operation.equals("read")) {
			if(parameters != null) {
				System.out.println("No parameters should be provided for the read command.");
			} else {
				outstandingAcks = rcp_read(parameters, seq_num);
				callback("read_callback", new String[]{"java.lang.String", "java.util.Set"}, new Object[]{parameters, outstandingAcks});
				commandsInProgress++;
			}
		} else if (operation.equals(COMMAND_START_TRANSACTION)){
			outstandingAcks = rcp_start_transaction(seq_num);
			callback("start_transaction_callback", new String[]{"java.util.Set"}, new Object[]{outstandingAcks});
			commandsInProgress++;

		} else if (operation.equals(COMMAND_COMMIT_TRANSACTION)){
			outstandingAcks = rcp_commit_transaction(seq_num);
			callback("commit_transaction_callback", new String[]{"java.util.Set"}, new Object[]{outstandingAcks});
			commandsInProgress++;

		} else {
			System.out.println("Unknown operation: " + operation);
		}
		updateSeqNum(outstandingAcks);
	}


	/*
	 * RPCALLS
	 * methods used to send RPCs to the server
	 */

	private void rpc_call(int node, int p, String msg, long seq_num){
		System.out.println("rcp_call sending message: " + node + " " + seq_num + " " + msg);
		Map<String, String> json_map = new TreeMap<String, String>();
		json_map.put(JSON_CURRENT_SEQ_NUM, Long.toString(seq_num));
		json_map.put(JSON_MSG, msg);
		json_map.put(JSON_REQUEST_ID, Long.toString(seq_num));
		json_map.put(JSON_TRANSACTION_ID, transaction_id);

		Type mapType = new TypeToken<Map<String, String>>() {}.getType();
		String json = objectToJson(json_map, mapType);

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
		if(commandsInProgress == 0 && !pending_commands.isEmpty()){
			if(state == TransactionState.COMMITTED){
				pending_commands.remove();
				transaction_id = INVALID_TID;
			}else if(state == TransactionState.ABORTED){
				transaction_id = INVALID_TID;
			}

			if(transaction_id.equals(INVALID_TID)){
				active_commands.clear();
				if(!pending_commands.isEmpty()){
					active_commands.add(COMMAND_START_TRANSACTION);
					active_commands.add(pending_commands.peek());
					active_commands.add(COMMAND_COMMIT_TRANSACTION);
				}
			}
		}
		if(commandsInProgress == 0 && !active_commands.isEmpty()){
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
			commandsInProgress--;
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
			commandsInProgress--;
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
			commandsInProgress--;
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
			commandsInProgress--;
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
			commandsInProgress--;
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

				System.out.println( val.substring(val.indexOf(TWEET_TIMESTAMP_TOKEN) + 1) + " : " + tweets.get(val));
			}
			commandsInProgress--;
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

			commandsInProgress--;
		} else {
			long min_ack = Long.MAX_VALUE;
			for(Long num : outstandingAcks){
				min_ack = Math.min(num, min_ack);
			}
			rcp_create(parameters, min_ack);
			callback("create_account_callback", new String[]{"java.lang.String", "java.util.Set"}, new Object[]{parameters, outstandingAcks});
		}
	}

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

			commandsInProgress--;
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

			commandsInProgress--;
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
	

	/*
	 * PAXOS STUFF
	 */
	
	private static final Type PAXOS_GROUP_TYPE =  new TypeToken<Set<Integer>>() {}.getType();

	// the JSON keys for messages paxos is sending
	private static final String JSON_COMMAND = "rpc";
	private static final String JSON_PAX_ROUND = "round";
	private static final String JSON_PAX_PROPOSAL_NUM = "n";
	private static final String JSON_PAX_VALUE = "value";
	private static final String JSON_PAX_GROUP_MEMBERS = "paxosMembers";
	private static final String JSON_PAX_KNOWN_SERVERS = "knownServers";

	// the different messages in the paxos protocol
	private static final String RPC_PAX_STORE_VALUE_REQUEST = "storeValueRequest";
	private static final String RPC_PAX_STORED_VALUE = "valueStored";
	private static final String RPC_PAX_PREPARE = "prepare";
	private static final String RPC_PAX_PROMISE = "promise";
	private static final String RPC_PAX_ACCEPT_REQUEST = "acceptRequest";	
	private static final String RPC_PAX_ACCEPTED = "accepted";
	private static final String RPC_PAX_LEARN = "learn";
	private static final String RPC_PAX_LEARN_ACQ = "learnAcq";
	private static final String RPC_PAX_RECOVER_FROM_ROUND = "needUpdatesStartingAt";
	private static final String RPC_PAX_JOIN_GROUP_REQUEST = "requestToJoinPaxos";
	private static final String RPC_PAX_JOIN_GROUP_CONFIRM = "joinRequestGranted";
	
	private static final String UPDATE_PAXOS_MEMBERSHIP_KEY = "paxosMembershipUpdate";
	private static final String PAXOS_ENTRY_POINT_MEMBER_KEY = "existingGroupMember";

	private PaxosModuel pax;
	private Set<Integer> knownServers;

	private void processMessageAsPaxos(byte[] msg, int sendingNode) {
		String msgJson = packetBytesToString(msg);
		System.out.println("msg received by paxos: " + msgJson);

		Type mapType = new TypeToken<Map<String, String>>() {}.getType();
		Map<String, String> msgMap = (Map<String, String>)jsonToObject(msgJson, mapType);
		String command = msgMap.get(JSON_COMMAND);
		String roundStr = msgMap.get(JSON_PAX_ROUND);
		long round = -1L;

		try{
			round = Long.parseLong(roundStr);
		} catch (Exception e) {
			// deliberately blank
		}

		//TODO short circuit if value already learned?
		if(pax.getLearnedValue(round) != null && !RPC_PAX_LEARN_ACQ.equals(command)){
			// we already learned a value, good2go
			Map<String, String> storedValueReply = new TreeMap<String, String>();
			storedValueReply.put(JSON_COMMAND, RPC_PAX_STORED_VALUE);
			storedValueReply.put(JSON_PAX_ROUND, roundStr);
			storedValueReply.put(JSON_PAX_VALUE, pax.getLearnedValue(round));
			paxosRpc(sendingNode, storedValueReply);
		}

		// server -> paxos
		if(RPC_PAX_STORE_VALUE_REQUEST.equals(command)) {
			// a server is requesting that a node 
			knownServers.add(sendingNode);

			//TODO detect duplicate response from same client,
			//TODO change to detect request from server
			long proposalNum = pax.startNewVote(sendingNode, round, msgMap.get(JSON_PAX_VALUE));

			//TODO howto deal with duplicate requests? implemented in startNewVote?
			if(proposalNum != -1L){
				// if a new round of voting is being started with this node as proposer
				Map<String, String> prepareMessage = new TreeMap<String, String>();
				prepareMessage.put(JSON_COMMAND, RPC_PAX_PREPARE);
				prepareMessage.put(JSON_PAX_PROPOSAL_NUM, Long.toString(proposalNum));
				prepareMessage.put(JSON_PAX_ROUND, roundStr);

				Set<Integer> nodes = pax.getPaxosGroup();

                /*
                //TODO REMOVE!!!!!!!!!!!
                nodes = new TreeSet<Integer>();
                nodes.add(1);
                nodes.add(2);
                nodes.add(3);
                */

                System.out.println("$$$$");
				for(Integer paxNode : nodes) {
                    System.out.println("paxNode"+paxNode);
					paxosRpc(paxNode, prepareMessage);
				}
			}
		} else if (RPC_PAX_RECOVER_FROM_ROUND.equals(command)) {
			knownServers.add(sendingNode);			
			Map<Long, String> updates = pax.getAllLearnedValues(round);
			
			//TODO send as a single packet?
			for(Long roundLearned : updates.keySet()){
				Map<String, String> updateMessage = new TreeMap<String,String>();
				updateMessage.put(JSON_COMMAND, RPC_PAX_RECOVER_FROM_ROUND);
				updateMessage.put(JSON_PAX_ROUND, roundLearned.toString());
				updateMessage.put(JSON_PAX_VALUE, updates.get(roundLearned));
				paxosRpc(sendingNode, updateMessage);
			}
			
			
			
			// paxos -> paxos
		} else if(RPC_PAX_PREPARE.equals(command)){ 
			String proposalNumStr = msgMap.get(JSON_PAX_PROPOSAL_NUM);
			long proposalNum = Long.parseLong(proposalNumStr);
			PaxosModuel.PrepareResponse res = pax.prepare(round, proposalNum);

			if(res!= null){
				// if we have not promised to honor a higher proposal #
				Map<String, String> response = new TreeMap<String, String>();
				response.put(JSON_COMMAND, RPC_PAX_PROMISE);
				response.put(JSON_PAX_ROUND, roundStr);
				response.put(JSON_PAX_PROPOSAL_NUM, Long.toString(res.n));
				response.put(JSON_PAX_VALUE, res.value);

				paxosRpc(sendingNode, response);
			}			
		} else if (RPC_PAX_PROMISE.equals(command)) {			
			boolean majorityPromised = pax.promise(round, 
					Long.parseLong(msgMap.get(JSON_PAX_PROPOSAL_NUM)), 
					msgMap.get(JSON_PAX_VALUE), 
					sendingNode);

			if(majorityPromised) {
				Map<String, String> message = new TreeMap<String, String>();
				message.put(JSON_COMMAND, RPC_PAX_ACCEPT_REQUEST);
				message.put(JSON_PAX_ROUND, roundStr);
				message.put(JSON_PAX_VALUE, pax.getProposedValue(round));
				message.put(JSON_PAX_PROPOSAL_NUM, Long.toString(pax.getProposalNumForRound(round)));

				sendToAllPaxos(message);
			}
		} else if (RPC_PAX_ACCEPT_REQUEST.equals(command)) {
			String proposalNumStr = msgMap.get(JSON_PAX_PROPOSAL_NUM);
			long proposalNum = Long.parseLong(proposalNumStr);
			boolean success = pax.propose(round, proposalNum, msgMap.get(JSON_PAX_VALUE));
			if(success){
				// if we are accepting that value
				Map<String, String> response = new TreeMap<String, String>();
				response.put(JSON_COMMAND, RPC_PAX_ACCEPTED);
				response.put(JSON_PAX_ROUND, roundStr);
				response.put(JSON_PAX_VALUE, msgMap.get(JSON_PAX_VALUE));

				paxosRpc(sendingNode, response);
			}
		} else if (RPC_PAX_ACCEPTED.equals(command)) {
			boolean majorityAccepted = pax.accepted(round, sendingNode);
			if(majorityAccepted) {	
				// if a quorum have accepted, send to learners
				Map<String, String> learnRequest = new TreeMap<String, String>();
				learnRequest.put(JSON_COMMAND, RPC_PAX_LEARN);
				learnRequest.put(JSON_PAX_ROUND, roundStr);
				learnRequest.put(JSON_PAX_VALUE, msgMap.get(JSON_PAX_VALUE));
				
				// send the value to the rest of the learners
				sendToAllPaxos(learnRequest);

				// learn the value ourselves
				pax.learn(round, pax.getProposedValue(round));				

				// send the confirmation back to the server
				// paxos -> server
				Map<String, String> txnConfirmMsg = new TreeMap<String, String>();
				txnConfirmMsg.put(JSON_COMMAND, RPC_PAX_STORED_VALUE);
				txnConfirmMsg.put(JSON_PAX_ROUND, roundStr);
				txnConfirmMsg.put(JSON_PAX_VALUE, msgMap.get(JSON_PAX_VALUE));
				
				//TODO just send to learners?
				//paxosRpc(pax.getProposingNodeId(round), txnConfirmMsg);
			}
		} else if (RPC_PAX_LEARN.equals(command) || RPC_PAX_STORED_VALUE.equals(command)) {
			pax.learn(round, msgMap.get(JSON_PAX_VALUE));
			Map<String, String> learnAck = new TreeMap<String, String>();
			learnAck.put(JSON_COMMAND, RPC_PAX_LEARN_ACQ);
			learnAck.put(JSON_PAX_ROUND, roundStr);

			paxosRpc(sendingNode, learnAck);

			// propagate the new transaction value to all of the FS servers
			Map<String, String> storedValueReply = new TreeMap<String, String>();
			storedValueReply.put(JSON_COMMAND, RPC_PAX_STORED_VALUE);
			storedValueReply.put(JSON_PAX_ROUND, roundStr);
			storedValueReply.put(JSON_PAX_VALUE, pax.getLearnedValue(round));

			sendToAllServers(storedValueReply);
		} else if (RPC_PAX_LEARN_ACQ.equals(command)){
			pax.learned(round, sendingNode);
		} else if (RPC_PAX_JOIN_GROUP_REQUEST.equals(command)){
			//TODO if the node is already part, send back membership
			if(pax.getPaxosGroup().contains(sendingNode)) {
				// we're done' it just needs to be informed of the results
				Map<String, String> membershipResponse = new TreeMap<String, String>();
				membershipResponse.put(JSON_COMMAND, RPC_PAX_JOIN_GROUP_CONFIRM);
				
				String groupJson = objectToJson(pax.getPaxosGroup(), PAXOS_GROUP_TYPE);
				
				membershipResponse.put(JSON_PAX_GROUP_MEMBERS, groupJson);
			} else {
				Map<String, String> membershipUpdateProposal = new TreeMap<String,String>();
				membershipUpdateProposal.put(UPDATE_PAXOS_MEMBERSHIP_KEY, "TRUE");
			}
			
		} else if (RPC_PAX_JOIN_GROUP_CONFIRM.equals(command)){
			
		} else {
			throw new IllegalArgumentException("unknown command: "+command);
		}
	}
	
	private void sendToAllServers(Map<String, String> message) {
		for(Integer serverId : knownServers) {
			paxosRpc(serverId, message);
		}
	}
	
	private void sendToAllPaxos(Map<String, String> message) {
		Set<Integer> nodes = pax.getPaxosGroup();
        System.out.println("PAXOS NDOES: " + nodes);
		for(Integer paxNode : nodes) {
			paxosRpc(paxNode, message);
		}
	}

	private void paxosRpc(int destNode, Map<String, String> message){
		message.put(JSON_CURRENT_SEQ_NUM, Long.toString(seq_num));

		Type mapType = new TypeToken<Map<String, String>>() {}.getType();
		String json = objectToJson(message, mapType);

		RIOSend(destNode, Protocol.PAXOS_PKT, Utility.stringToByteArray(json));
		System.out.printf("Paxos to %d: %s\n", destNode,json);
	}
	
	//TODO note to self, just use paxos to join new nodes into paxos
	//TODO hory crapp
	//TODO make sure servers save state for latest round#, dont redo updates
}
