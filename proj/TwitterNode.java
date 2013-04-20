import edu.washington.cs.cse490h.lib.*;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.util.*;

public class TwitterNode extends RIONode {

    public static double getFailureRate() { return 2/100.0; }
    public static double getRecoveryRate() { return 2/100.0; }
    public static double getDropRate() { return 2/100.0; }
    public static double getDelayRate() { return 0/100.0; }

    private static boolean failed;

    private Map<Long, Boolean> acked;
    private Integer from;
    private int protocol;
    private byte[] msg;

    private long seq_num;
    //private Set<Long>  outstanding_ack;

    @Override
    public void onRIOReceive(Integer from, int protocol, byte[] msg) {
        //msg from server
        if(from == 0) {
            //Client
            String response =  packetBytesToString(msg);
            System.out.println("message received by client: " + response);

            String results = "";
            long response_seq_num = Long.parseLong(response.split("\\s")[0]);
            seq_num = Long.parseLong(response.split("\\s")[1]);

            try {
                results = response.substring(response.split("\\s")[0].length() + 1);
            }catch(Exception e){
                //no results
            }

            System.out.println("server response: " + results);

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
                    }catch(Exception e){

                    }
                }
            }else if(command.equals("append")) {
                //TODO impliment method
                response += "todo";
            }else if(command.equals("read")) {
                try{
                    reader = super.getReader(filename);
                    response += reader.readLine();
                }catch(Exception e){

                }

            }else if(command.equals("delete")){
                //TODO impliment method
                response += "todo";
            }else{
                response += "unknown command: " + command;
            }
            if(reader != null){
                try{
                    reader.close();
                }catch(Exception e){

                }

                if(writer != null){
                    try{
                        writer.close();
                    }catch(Exception e){

                    }
                }
            }
            System.out.println("Server sending response: " + request_seq_num);
            RIOSend(1, Protocol.RIOTEST_PKT, response.getBytes());
        }
    }

    @Override
    public void start() {
        logOutput("Starting up...");

        // Generate a user-level synoptic event to indicate that the node started.
        logSynopticEvent("started");

        //outstanding_ack = new HashSet<Long>();
        acked = new HashMap<Long, Boolean>();
        seq_num = System.currentTimeMillis();
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

        Set<Long> outstandingAcks = null;
        
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
            	//TODO how do we handle logins/logouts?
                outstandingAcks = rcp_login(parameters, seq_num);
                callback("login_callback", new String[]{"java.lang.String", "java.util.Set"}, new Object[]{parameters, outstandingAcks});
            }

        } else if(operation.equals("logout")) {
        	if(parameters == null) {
                // no username specified
                System.out.println("No username specified. Unable to logout.");
            } else {
                //TODO how do we handle logins/logouts?
                outstandingAcks = rcp_logout(parameters, seq_num);
                callback("logout_callback", new String[]{"java.lang.String", "java.util.Set"}, new Object[]{parameters, outstandingAcks});
            }

        } else if(operation.equals("post")) {
            if(parameters == null) {
                // no tweet message
                System.out.println("No message specified. Unable to post tweet.");
            } else {
                outstandingAcks = rcp_post(parameters, seq_num);
                callback("post_callback", new String[]{"java.lang.String", "java.util.Set"}, new Object[]{parameters, outstandingAcks});
            }
        } else if(operation.equals("add")) {
            if(parameters == null) {
                // no username to follow
                System.out.println("No username specified. Unable to follow user.");
            } else {
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
            if(parameters == null) {
                // no username to unfollow
                System.out.println("No username specified. Unable to unfollow user.");
            } else {
                outstandingAcks = rcp_read(parameters, seq_num);
                callback("read_callback", new String[]{"java.lang.String", "java.util.Set"}, new Object[]{parameters, outstandingAcks});
            }

        } else {
        	System.out.println("Unknown operation: " + operation);
        }
        for(Long val : outstandingAcks){
            seq_num = Math.max(seq_num, val);
        }
        seq_num++;
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

        rpc_call(0, Protocol.RIOTEST_PKT, "create " + parameters + "-tweets", seq_num);
        rpc_call(0, Protocol.RIOTEST_PKT, "create " + parameters + "-following", seq_num + 1);
        rpc_call(0, Protocol.RIOTEST_PKT, "create " + parameters + "-info", seq_num + 2);
        Set<Long> returned = new TreeSet<Long>();
        returned.add(seq_num);
        returned.add(seq_num + 1);
        returned.add(seq_num + 2);
        return returned;
    }

    private Set<Long> rcp_login(String parameters, long seq_num){
        //TODO call rpc_call() method
        Set<Long> returned = new TreeSet<Long>();
        rpc_call(0, Protocol.RIOTEST_PKT, "read " + parameters + "-info", seq_num);
        acked.put(seq_num, false);
        returned.add(seq_num);
        return returned;
    }

    private Set<Long> rcp_logout(String parameters, long seq_num){
        //TODO call rpc_call() method
        //Send RPC call to server to log user out
        Set<Long> returned = new TreeSet<Long>();
        return returned;
    }

    private Set<Long> rcp_post(String parameters, long seq_num){
        //TODO call rpc_call() method
        //When we send a post to the server, how does it know which account is posting?
        //Maybe include username in post request
        Set<Long> returned = new TreeSet<Long>();
        rpc_call(0, Protocol.RIOTEST_PKT, "append " + parameters, seq_num);
        return returned;
    }

    private Set<Long> rcp_add(String parameters, long seq_num){
        //TODO call rpc_call() method
        Set<Long> returned = new TreeSet<Long>();
        return returned;
    }

    private Set<Long> rcp_delete(String parameters, long seq_num){
        //TODO call rpc_call() method
        Set<Long> returned = new TreeSet<Long>();
        return returned;

    }

    private Set<Long> rcp_read(String parameters, long seq_num){
        //TODO call rpc_call() method
        Set<Long> returned = new TreeSet<Long>();
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
            acked.clear();
            String response = packetBytesToString(this.msg);
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
            callback("create_callback", new String[]{"java.lang.String"}, new Object[]{parameters});
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