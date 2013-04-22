import edu.washington.cs.cse490h.lib.*;

import java.io.*;
import java.util.*;

public class TwitterNode extends RIONode {

    public static double getFailureRate() { return 0/100.0; }
    public static double getRecoveryRate() { return 0/100.0; }
    public static double getDropRate() { return 0/100.0; }
    public static double getDelayRate() { return 0/100.0; }

    private static boolean failed;

    private Map<Long, Boolean> acked;
    private Integer from;
    private int protocol;
    private byte[] msg;

    private long seq_num;

    private String username;
    private Map<String, String> tweets;

    private static final String TWEET_FILE_SUFFIX = "-tweets";
    private static final String FOLLOWERS_FILE_SUFFIX = "-following";
    private static final String INFO_FILE_SUFFIX = "-info";

    @Override
    public void onRIOReceive(Integer from, int protocol, byte[] msg) {
        //msg from server
        if(from == 0) {
            //Client
            String response =  packetBytesToString(msg);
            System.out.println("message received by client: " + response);

            String results = "";
            long response_seq_num = Long.parseLong(response.split("\\s")[0]);
            seq_num = Math.max(Long.parseLong(response.split("\\s")[1]), seq_num);

            try {
                results = response.substring(response.split("\\s")[0].length() + response.split("\\s")[1].length() + 2);
            }catch(Exception e){
                //no results
            }

            System.out.println("server response: " + results);
            String command = results.split("\\s")[0];
            //check to see if more RCP calls need to be sent
            if(command.equals("read")){
                //read response
                //check if it is a read of a '-following' file or '-tweets'
                String filename = results.split("\\s")[1];

                if(filename.endsWith("-following")){
                    //Send more RCP calls to fetch the tweets
                    Set<String> usernames = new HashSet<String>();
                    for(int i = 2; i < results.split("\\s").length; i++){
                        String username = results.split("\\s")[i];
                        usernames.add(username);
                    }
                    Set<Long> outstandingAcks = rcp_read_multiple(usernames, seq_num);

                //   String parameters = filename;
                    callback("read_multiple_callback", new String[]{"java.util.Set", "java.util.Set"}, new Object[]{usernames, outstandingAcks});
                    updateSeqNum(outstandingAcks);
                }else if(filename.endsWith("-tweets")){
                    //Return tweets to user
                    String all_tweets = results.substring(results.split("\\s")[0].length() + results.split("\\s")[1].length() + 2);
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
            this.from = from;
            this.protocol = protocol;
            this.msg = msg;
            acked.put(response_seq_num, true);
        }
        //msg from client
        if(from == 1){
            //Server
            String message = packetBytesToString(msg);
            System.out.println("message received by server: " + message);
            String request_seq_num = message.split("\\s")[0];
            String command = message.split("\\s")[1];
            String filename =  message.split("\\s")[2];

            String response = request_seq_num + " " + seq_num + " ";
            PersistentStorageWriter writer = null;
            PersistentStorageReader reader = null;
            PersistentStorageOutputStream byte_writer= null;
            PersistentStorageInputStream byte_reader = null;
            if(command.equals("create")) {
              //  boolean file_exists = Utility.fileExists(this, filename);
                boolean  file_exists = false; //TODO currently overwrites existing files
                if(file_exists){
                    //fail
                    response += "username_taken";
                }else{
                    response += "okay";
                    try{
                        boolean append = false;
                        writer = super.getWriter(filename, append);
                        writer.write("");

                        byte_writer = super.getOutputStream(filename, append);
                        ObjectOutputStream out = new ObjectOutputStream(byte_writer);
                        TreeMap<String, String> fileMap = new TreeMap<String, String>();
                        out.writeObject(fileMap);
                        out.close();

                    }catch(Exception e){

                    }
                }
            }else if(command.equals("append")) {
                try{
                    boolean append = false;
                    byte_reader = super.getInputStream(filename);
                    ObjectInputStream in = new ObjectInputStream(byte_reader);
                    TreeMap<String, String> fileMap = (TreeMap<String, String>) in.readObject();
                    in.close();

                    if(!fileMap.containsKey(request_seq_num)){
                        //duplicate request
                        byte_writer = super.getOutputStream(filename, append);
                        ObjectOutputStream out = new ObjectOutputStream(byte_writer);
                        String tweet = message.substring(request_seq_num.length() + command.length() + filename.length() + 3);
                        fileMap.put(request_seq_num, tweet);
                        out.writeObject(fileMap);
                        out.close();
                    } else {
                        System.out.println("Append not processed, timestamp already exists: " + message);
                    }

                    //debug
                    System.out.println("MAP VALUES (append): ");
                    System.out.println(fileMap.values());
                }catch(Exception e){
                    System.out.println();
                    e.printStackTrace();
                }
                response += "okay";
            }else if(command.equals("read")) {
                try{
                    response += "read " + filename + " ";
                    byte_reader = super.getInputStream(filename);
                    ObjectInputStream in = new ObjectInputStream(byte_reader);
                    TreeMap<String, String> fileMap = (TreeMap<String, String>) in.readObject();
                    in.close();

                    String username = filename.split("-")[0];
                    for(String s : fileMap.keySet()){
                        //return values -- username of people you are following
                   //     response += s + " " + username + " " + fileMap.get(s) + "\n";
                        //TODO: return username with tweet!!!!!

                        response += s + username + " " + fileMap.get(s) + "\n";
                    }
                    response = response.trim();
                }catch(Exception e){

                }
            }else if(command.equals("delete")){
                //Removing followers from "-followers" file
                //If user is not currently being followed -- does nothing
                //If user is being followed multiple times -- removes all ocurences
                try{
                    //read in treemap from file
                    boolean append = false;
                    byte_reader = super.getInputStream(filename);
                    ObjectInputStream in = new ObjectInputStream(byte_reader);
                    TreeMap<String, String> fileMap = (TreeMap<String, String>) in.readObject();
                    in.close();

                    String unfollow_username = message.substring(request_seq_num.length() + command.length() + filename.length() + 3);
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

                        //only need to write if we made modifications
                        byte_writer = super.getOutputStream(filename, append);
                        ObjectOutputStream out = new ObjectOutputStream(byte_writer);
                        out.writeObject(fileMap);
                        out.close();
                    }

                    //debug
                    System.out.println("MAP VALUES (remove): ");
                    System.out.println(fileMap.values());
                }catch(Exception e){
                    e.printStackTrace();
                }
                response += "okay";
            }else{
                response += "unknown command: " + command;
            }
            if(reader != null){
                try{
                    reader.close();
                }catch(Exception e){

                }
            }
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
            if(byte_writer != null){
                try{
                    byte_writer.close();
                }catch(Exception e){

                }
            }

            System.out.println("Server sending response: " + request_seq_num);
            RIOSend(1, Protocol.RIOTEST_PKT, response.getBytes());
        }
    }

    private void updateSeqNum(Set<Long> outstandingAcks){
        //Find the max of the outstanding acks sent
        for(Long val : outstandingAcks){
            seq_num = Math.max(seq_num, val);
        }
        //Next unused sequence number
        seq_num++;
    }

    private byte[] read_file(PersistentStorageInputStream byte_reader){
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try{
            byte[] next_bytes = new byte[1024];
            int bytesRead = 0;
            while(bytesRead > 0){
                bytesRead = byte_reader.read(next_bytes);
                bytes.write(next_bytes, 0, bytesRead);
            }
        }catch(IOException e){

        }
        return bytes.toByteArray();
    }

    @Override
    public void start() {
        logOutput("Starting up...");

        // Generate a user-level synoptic event to indicate that the node started.
        logSynopticEvent("started");

        //outstanding_ack = new HashSet<Long>();
        acked = new HashMap<Long, Boolean>();
        seq_num = System.currentTimeMillis();
        tweets = new HashMap<String, String>();
    }

    @Override
    public void onCommand(String command) {
        String[] split = command.split("\\s");
        String operation = split[0];
        String parameters = null;
        try {
            parameters = command.substring(operation.length() + 1);
        }catch(Exception e){
            //no parameters
        }

        /*
        Twitter requirements from project 1 write up:
        Create a user
        Login/logout as user
        Post to a twitter stream
        Add/delete a follower to/from a twitter stream
        Read all (unread) posts that a user is following
        */

        //Server = 0
        //Client = 1

        Set<Long> outstandingAcks = new TreeSet<Long>();
        
        if (operation.equals("create")) {
            if(parameters == null) {
                // no username specified
                System.out.println("No username specified. Unable to create account.");
            } else {
                outstandingAcks = rcp_create(parameters, seq_num);
                callback("create_callback", new String[]{"java.lang.String", "java.util.Set"}, new Object[]{parameters, outstandingAcks});
            }
        } else if(operation.equals("login")) {
        	if(parameters == null) {
                // no username specified
                System.out.println("No username specified. Unable to login.");
            } else {
                username = parameters;
                outstandingAcks = rcp_login(parameters, seq_num);
                callback("login_callback", new String[]{"java.lang.String", "java.util.Set"}, new Object[]{parameters, outstandingAcks});
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
            }
        } else if(operation.equals("add")) {
            if(parameters == null) {
                // no username to follow
                System.out.println("No username specified. Unable to follow user.");
            } else {
                //TODO: does not check if user exists, and allows uses to follow same user multiple times
                outstandingAcks = rcp_add(parameters, seq_num);
                callback("add_callback", new String[]{"java.lang.String", "java.util.Set"}, new Object[]{parameters, outstandingAcks});
            }
        } else if(operation.equals("delete")) {
            if(parameters == null) {
                // no username to unfollow
                System.out.println("No username specified. Unable to unfollow user.");
            } else {
                outstandingAcks = rcp_delete(parameters, seq_num);
                callback("delete_callback", new String[]{"java.lang.String", "java.util.Set"}, new Object[]{parameters, outstandingAcks});
            }
        } else if(operation.equals("read")) {
            if(parameters != null) {
                System.out.println("No parameters should be provided for the read command.");
            } else {
                outstandingAcks = rcp_read(parameters, seq_num);
                callback("read_callback", new String[]{"java.lang.String", "java.util.Set"}, new Object[]{parameters, outstandingAcks});
            }
        } else {
        	System.out.println("Unknown operation: " + operation);
        }
        updateSeqNum(outstandingAcks);
    }

    private void rpc_call(int node, int p,String msg, long seq_num){
        System.out.println("rcp_call sending message: " + seq_num + " " + msg);
        RIOSend(node, p, Utility.stringToByteArray(seq_num + " " + msg));
    }

    private void callback(String methodName, String[] paramTypes, Object[] params) {
        try {
            Callback cb = new Callback(Callback.getMethod(methodName, this, paramTypes),
                    this, params);
            addTimeout(cb, 10);
        } catch (NoSuchMethodException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    private Set<Long> rcp_create(String parameters, long seq_num){
        rpc_call(0, Protocol.RIOTEST_PKT, "create " + parameters + TWEET_FILE_SUFFIX, seq_num);
        rpc_call(0, Protocol.RIOTEST_PKT, "create " + parameters + FOLLOWERS_FILE_SUFFIX, seq_num + 1);
        rpc_call(0, Protocol.RIOTEST_PKT, "create " + parameters + INFO_FILE_SUFFIX, seq_num + 2);
        Set<Long> returned = new TreeSet<Long>();
        returned.add(seq_num);
        returned.add(seq_num + 1);
        returned.add(seq_num + 2);
        return returned;
    }

    private Set<Long> rcp_login(String parameters, long seq_num){
        Set<Long> returned = new TreeSet<Long>();
        rpc_call(0, Protocol.RIOTEST_PKT, "read " + parameters + INFO_FILE_SUFFIX, seq_num);
        acked.put(seq_num, false);
        returned.add(seq_num);
        return returned;
    }

    /*
    //Not sending RCP call to server for log out
    private Set<Long> rcp_logout(String parameters, long seq_num){
        //TODO call rpc_call() method
        //Send RPC call to server to log user out
        Set<Long> returned = new TreeSet<Long>();
        return returned;
    }
    */

    private Set<Long> rcp_post(String parameters, long seq_num){
        Set<Long> returned = new TreeSet<Long>();
        rpc_call(0, Protocol.RIOTEST_PKT, "append " + username + TWEET_FILE_SUFFIX + " " + parameters, seq_num);
        acked.put(seq_num, false);
        returned.add(seq_num);
        return returned;
    }

    private Set<Long> rcp_add(String parameters, long seq_num){
        Set<Long> returned = new TreeSet<Long>();
        rpc_call(0, Protocol.RIOTEST_PKT, "append " + username + FOLLOWERS_FILE_SUFFIX + " " + parameters, seq_num);
        acked.put(seq_num, false);
        returned.add(seq_num);
        return returned;
    }

    private Set<Long> rcp_delete(String parameters, long seq_num){
        Set<Long> returned = new TreeSet<Long>();
        rpc_call(0, Protocol.RIOTEST_PKT, "delete " + username + FOLLOWERS_FILE_SUFFIX + " " + parameters, seq_num);
        acked.put(seq_num, false);
        returned.add(seq_num);
        return returned;
    }

    private Set<Long> rcp_read(String parameters, long seq_num){
        Set<Long> returned = new TreeSet<Long>();
        rpc_call(0, Protocol.RIOTEST_PKT, "read " + username + FOLLOWERS_FILE_SUFFIX, seq_num);
        acked.put(seq_num, false);
        returned.add(seq_num);
        return returned;
    }

    //sends multiple RCP read requests to get tweets from all users being followed
    private Set<Long> rcp_read_multiple(Set<String> usernames, long seq_num){
        Set<Long> returned = new TreeSet<Long>();
        int count = 0;
        for(String username: usernames){
            rpc_call(0, Protocol.RIOTEST_PKT, "read " + username + TWEET_FILE_SUFFIX, seq_num + count);
            returned.add(seq_num + count);
        }

        return returned;
    }


    public void login_callback(String parameters, Set<Long> outstandingAcks) {
    	//TODO test
    	System.out.println("login_callback called: " + parameters);
        boolean all_acked = allAcked(outstandingAcks);

        if(all_acked) {
            for(Long ack : outstandingAcks) {
                acked.remove(ack);
            }
            String response = packetBytesToString(this.msg);
            //TODO stuff
        } else {
        	// retry
            callback("login_callback", new String[]{"java.lang.String", "java.util.Set"}, new Object[]{parameters, outstandingAcks});
        }
    }
    
    public void logout_callback(String parameters, Set<Long> outstandingAcks) {
    	//TODO test
    	System.out.println("logout_callback called: " + parameters);
        boolean all_acked = allAcked(outstandingAcks);
        if(all_acked) {
            for(Long ack : outstandingAcks) {
                acked.remove(ack);
            }
            String response = packetBytesToString(this.msg);
            //TODO stuffhere
        } else {
            //TODO retry
            callback("logout_callback", new String[]{"java.lang.String", "java.util.Set"}, new Object[]{parameters, outstandingAcks});
        }
    }
    
    public void delete_callback(String parameters, Set<Long> outstandingAcks) {
    	//TODO test
    	System.out.println("delete_callback called: " + parameters);
        boolean all_acked = allAcked(outstandingAcks);

        if(all_acked) {
            for(Long ack : outstandingAcks) {
                acked.remove(ack);
            }            String response = packetBytesToString(this.msg);
            //TODO stuff
        } else {
        	// retry
            callback("delete_callback", new String[]{"java.lang.String", "java.util.Set"}, new Object[]{parameters, outstandingAcks});
        }
    }
    
    public void post_callback(String parameters, Set<Long> outstandingAcks) {
    	//TODO test
    	System.out.println("post_callback called: " + parameters);
        boolean all_acked = allAcked(outstandingAcks);

        if(all_acked) {
            for(Long ack : outstandingAcks) {
                acked.remove(ack);
            }
            String response = packetBytesToString(this.msg);
            //TODO stuff
        } else {
        	// retry
            callback("post_callback", new String[]{"java.lang.String", "java.util.Set"}, new Object[]{parameters, outstandingAcks});
        }
    }
    
    public void add_callback(String parameters, Set<Long> outstandingAcks) {
    	//TODO test
    	System.out.println("login_callback called: " + parameters);
        boolean all_acked = allAcked(outstandingAcks);

        if(all_acked) {
            for(Long ack : outstandingAcks) {
                acked.remove(ack);
            }
            String response = packetBytesToString(this.msg);
            //TODO stuff
        } else {
        	// retry
            callback("add_callback", new String[]{"java.lang.String", "java.util.Set"}, new Object[]{parameters, outstandingAcks});
        }
    }
    
    public void read_callback(String parameters, Set<Long> outstandingAcks) {
    	//TODO test
    	System.out.println("read_callback called");
        boolean all_acked = allAcked(outstandingAcks);

        if(all_acked) {
            for(Long ack : outstandingAcks) {
                acked.remove(ack);
            }
            String response = packetBytesToString(this.msg);
            //TODO stuff
        } else {
        	// retry
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
            String response = packetBytesToString(this.msg);
            System.out.println(username + "'s follower's tweets:");
            for(String val : tweets.keySet()){
                System.out.println("Tweet: " + val + " : " + tweets.get(val));
            }
        } else {
            long min_ack = Long.MAX_VALUE;
            for(Long num : outstandingAcks){
                min_ack = Math.min(num, min_ack);
            }
            rcp_read_multiple(parameters, min_ack);
            callback("read_multiple_callback", new String[]{"java.util.Set", "java.util.Set"}, new Object[]{parameters, outstandingAcks});
        }

    }

    public void create_callback(String parameters, Set<Long> outstandingAcks) {
        System.out.println("create_callback called: " + parameters);
        boolean all_acked = allAcked(outstandingAcks);

        if(all_acked) {
            for(Long ack : outstandingAcks) {
                acked.remove(ack);
            }
            String response = packetBytesToString(this.msg);
            if(response.equals("username_taken")){   //TODO: fix or remove
                System.out.println("Username: " + parameters + "taken. Try again.");
            } else {
                System.out.println("Account created!");
                System.out.println("response: " + response);
            }
        } else {
            long min_ack = Long.MAX_VALUE;
            for(Long num : outstandingAcks){
                min_ack = Math.min(num, min_ack);
            }
            rcp_create(parameters, min_ack);
            callback("create_callback", new String[]{"java.lang.String", "java.util.Set"}, new Object[]{parameters, outstandingAcks});
        }
    }

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
        if (failed) {
            return "FAILED!!!\n" + super.toString();
        } else {
            return super.toString();
        }
    }
}