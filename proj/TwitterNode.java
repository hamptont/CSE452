import edu.washington.cs.cse490h.lib.Callback;
import edu.washington.cs.cse490h.lib.Utility;

import java.io.PrintStream;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class TwitterNode extends RIONode {

    private boolean failed = false;

    private Map<Integer, Boolean> acked;
    private Integer from;
    private int protocol;
    private byte[] msg;

    private int seq_num;
    private Set<Integer>  outstanding_ack;

    @Override
    public void onRIOReceive(Integer from, int protocol, byte[] msg) {
        //msg from server
        if(from == 0) {
            //Client
            System.out.println("message received by client: " + packetBytesToString(msg));
            this.from = from;
            this.protocol = protocol;
            this.msg = msg;
            acked.put(seq_num, true);
        }
        //msg from client
        if(from == 1){
            //Server
            String message = packetBytesToString(msg);
            System.out.println("message received by server: " + message);
            String command = message.split("\\s")[1];
            String response = "";
            if(command.equals("create")) {
                response = "success";
                //TODO create files
                //check that username is not taken, else return failure
            }else if(command.equals("append")) {
                //TODO impliment method
                response = "todo";
            }else if(command.equals("read")) {
                //TODO impliment method
                response = "todo";
            }else if(command.equals("delete")){
                //TODO impliment method
                response = "todo";
            }else{
                response = "unknown command: " + command;
            }
            RIOSend(1, Protocol.RIOTEST_PKT, response.getBytes());
        }
    }

    @Override
    public void start() {
        logOutput("Starting up...");

        // Generate a user-level synoptic event to indicate that the node started.
        logSynopticEvent("started");

        outstanding_ack = new HashSet<Integer>();
        acked = new HashMap<Integer, Boolean>();
        seq_num = -1;
    }

    @Override
    public void onCommand(String command) {
        String[] split = command.split("\\s");
        String operation = split[0];
        String parameters = null;
        try {
            parameters = command.substring(operation.length() + 1);
        }catch(Exception e){
            //no filename
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
        
        if (operation.equals("create")) {
            if(parameters == null) {
                // no username specified
                System.out.println("No username specified. Unable to create account.");
            } else {
                rcp_create(parameters);
                callback("create_callback", new String[]{"java.lang.String"}, new Object[]{parameters});
            }


        //private void callback(String methodName, String[] paramTypes, Object[] params) {

        } else if(operation.equals("login")) {
        	if(parameters == null) {
                // no username specified
                System.out.println("No username specified. Unable to login.");
            } else {
            	//TODO how do we handle logins/logouts?
                rcp_login(parameters);
                callback("login_callback", new String[]{"java.lang.String"}, new Object[]{parameters});
            }

        } else if(operation.equals("logout")) {
        	if(parameters == null) {
                // no username specified
                System.out.println("No username specified. Unable to logout.");
            } else {
                //TODO how do we handle logins/logouts?
                rcp_logout(parameters);
                callback("logout_callback", new String[]{"java.lang.String"}, new Object[]{parameters});
            }

        } else if(operation.equals("post")) {
            if(parameters == null) {
                // no tweet message
                System.out.println("No message specified. Unable to post tweet.");
            } else {
                rcp_post(parameters);
                callback("post_callback", new String[]{"java.lang.String"}, new Object[]{parameters});
            }
        } else if(operation.equals("add")) {
            if(parameters == null) {
                // no username to follow
                System.out.println("No username specified. Unable to follow user.");
            } else {
                rcp_add(parameters);
                callback("add_callback", new String[]{"java.lang.String"}, new Object[]{parameters});
            }
        } else if(operation.equals("delete")) {
            if(parameters == null) {
                // no username to unfollow
                System.out.println("No username specified. Unable to unfollow user.");
            } else {
                rcp_delete(parameters);
                callback("delete_callback", new String[]{"java.lang.String"}, new Object[]{parameters});
            }
        } else if(operation.equals("read")) {
            rcp_read();
        	callback("read_callback", new String[0], new Object[0]);
        } else {
        	System.out.println("Unknown operation: " + operation);
        }
    }

    private void rpc_call(int node, int p,String msg){
        System.out.println("rcp_call sending message: " + seq_num + " " + msg);
        RIOSend(node, p, Utility.stringToByteArray(seq_num + " " + msg));
        acked.put(seq_num, false);
        outstanding_ack.add(seq_num);
        seq_num++;
    }

    private void callback(String methodName, String[] paramTypes, Object[] params) {
        try {
            Callback cb = new Callback(Callback.getMethod(methodName, this, paramTypes),
                    this, params);
            addTimeout(cb, 3);
        } catch (NoSuchMethodException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    private void rcp_create(String parameters){
        rpc_call(0, Protocol.RIOTEST_PKT, "create " + parameters + "-tweets");
        rpc_call(0, Protocol.RIOTEST_PKT, "create " + parameters + "-following");
        rpc_call(0, Protocol.RIOTEST_PKT, "create " + parameters + "-info");
    }

    private void rcp_login(String parameters){
        //TODO call rpc_call() method
        //Send RPC call to get current seq number for account?
        //need to figure out how to handle login/logout
    }

    private void rcp_logout(String parameters){
        //TODO call rpc_call() method
        //Send RPC call to server to log user out
    }

    private void rcp_post(String parameters){
        //TODO call rpc_call() method
        //When we send a post to the server, how does it know which account is posting?
        //Maybe include username in post request
        rpc_call(0, Protocol.RIOTEST_PKT, "append " + parameters);
    }

    private void rcp_add(String parameters){
        //TODO call rpc_call() method
    }

    private void rcp_delete(String parameters){
        //TODO call rpc_call() method

    }

    private void rcp_read(){
        //TODO call rpc_call() method

    }

    public void login_callback(String parameters) {
    	//TODO test
    	System.out.println("login_callback called: " + parameters);
        boolean all_acked = allAcked();
        outstanding_ack.clear();
        acked.clear();
        if(all_acked) {
            String response = packetBytesToString(this.msg);
            //TODO stuff
        } else {
        	// retry
            callback("login_callback", new String[]{"java.lang.String"}, new Object[]{parameters});
        }
    }
    
    public void logout_callback(String parameters) {
    	//TODO test
    	System.out.println("logout_callback called: " + parameters);
        boolean all_acked = allAcked();
        outstanding_ack.clear();
        acked.clear();
        if(all_acked) {
            String response = packetBytesToString(this.msg);
            //TODO stuffhere
        } else {
            //TODO retry
            callback("logout_callback", new String[]{"java.lang.String"}, new Object[]{parameters});
        }
    }
    
    public void delete_callback(String parameters) {
    	//TODO test
    	System.out.println("delete_callback called: " + parameters);
        boolean all_acked = allAcked();
        outstanding_ack.clear();
        acked.clear();
        if(all_acked) {
            String response = packetBytesToString(this.msg);
            //TODO stuff
        } else {
        	// retry
            callback("delete_callback", new String[]{"java.lang.String"}, new Object[]{parameters});
        }
    }
    
    public void post_callback(String parameters) {
    	//TODO test
    	System.out.println("post_callback called: " + parameters);
        boolean all_acked = allAcked();
        outstanding_ack.clear();
        acked.clear();
        if(all_acked) {
            String response = packetBytesToString(this.msg);
            //TODO stuff
        } else {
        	// retry
            callback("post_callback", new String[]{"java.lang.String"}, new Object[]{parameters});
        }
    }
    
    public void add_callback(String parameters) {
    	//TODO test
    	System.out.println("login_callback called: " + parameters);
        boolean all_acked = allAcked();
        outstanding_ack.clear();
        acked.clear();
        if(all_acked) {
            String response = packetBytesToString(this.msg);
            //TODO stuff
        } else {
        	// retry
            callback("add_callback", new String[]{"java.lang.String"}, new Object[]{parameters});
        }
    }
    
    public void read_callback() {
    	//TODO test
    	System.out.println("read_callback called");
        boolean all_acked = allAcked();
        outstanding_ack.clear();
        acked.clear();
        if(all_acked) {
            String response = packetBytesToString(this.msg);
            //TODO stuff
        } else {
        	// retry
            callback("read_callback", new String[0], new Object[0]);
        }
    }

    public void create_callback(String parameters) {
        System.out.println("create_callback called: " + parameters);
        boolean all_acked = allAcked();
        outstanding_ack.clear();
        acked.clear();
        if(all_acked) {
            String response = packetBytesToString(this.msg);
            if(response.equals("username_taken")){
                System.out.println("Username: " + parameters + "taken. Try again.");
            } else {
                System.out.println("Account created!");
                System.out.println("response: " + response);
            }
        } else {
            rcp_create(parameters);
            callback("create_callback", new String[]{"java.lang.String"}, new Object[]{parameters});
        }
    }

    private boolean allAcked() {
        boolean all_acked = true;
        for(Integer i : outstanding_ack) {
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