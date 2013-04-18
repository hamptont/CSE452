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
        //client waits for ACK
        System.out.println("onRIOReceive");

        //msg from server
        if(from == 0) {
            //Client
            this.from = from;
            this.protocol = protocol;
            this.msg = msg;
        //    this.acked = true;
            acked.put(seq_num, true);
        }
        //msg from client
        if(from == 1){
            //Server
            String message = packetBytesToString(msg);
            String command = message.split("\\s")[1];
            System.out.println("message: " + command);
            String response = "unknown command: " + command;
            if(command.equals("create")) {
                response = "success";
                //TODO create files
                //check that username is not taken, else return failure
            }else if(command.equals("append")) {
                response = "todo";
            }else if(command.equals("read")) {
                response = "todo";
            }else if(command.equals("delete")){
                response = "todo";
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
                System.out.println("RIOSEND");

                create_userfiles(parameters);
            }

            callback("create_callback", new String[]{"java.lang.String"}, new Object[]{parameters});

        //private void callback(String methodName, String[] paramTypes, Object[] params) {

        }else if(operation.equals("login")) {

        }else if(operation.equals("logout")) {

        }else if(operation.equals("post")) {

        }else if(operation.equals("add")) {

        }else if(operation.equals("delete")) {

        }else if(operation.equals("read")) {

        }
    }

    private void create_userfiles(String parameters){
        rpc_call(0, Protocol.RIOTEST_PKT, "create " + parameters + "-tweets");
        rpc_call(0, Protocol.RIOTEST_PKT, "create " + parameters + "-following");
        rpc_call(0, Protocol.RIOTEST_PKT, "create " + parameters + "-info");
    }

    private void rpc_call(int node, int p,String msg){
        RIOSend(node, p, Utility.stringToByteArray(seq_num + " " + msg));
        acked.put(seq_num, false);
        outstanding_ack.add(seq_num);
        seq_num++;

    }

    private void callback(String methodName, String[] paramTypes, Object[] params) {
        try {
            System.out.println(params);
            Callback cb = new Callback(Callback.getMethod(methodName, this, paramTypes),
                    this, params);
            addTimeout(cb, 10);
        } catch (NoSuchMethodException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    public void create_callback(String parameters) {
        System.out.println("create_callback called: " + parameters);
        boolean all_acked = true;
        for(Integer i : outstanding_ack) {
            if(!acked.get(i)){
                all_acked = false;
            }
        }
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
            create_userfiles(parameters);
            callback("create_callback", new String[]{"java.lang.String"}, new Object[]{parameters});
        }
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