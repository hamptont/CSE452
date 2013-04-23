import java.lang.reflect.Method;
import java.util.*;

import edu.washington.cs.cse490h.lib.Callback;
import edu.washington.cs.cse490h.lib.Utility;

/**
 * Layer above the basic messaging layer that provides reliable, in-order
 * delivery in the absence of faults. This layer does not provide much more than
 * the above.
 * 
 * At a minimum, the student should extend/modify this layer to provide
 * reliable, in-order message delivery, even in the presence of node failures.
 */
public class ReliableInOrderMsgLayer {
	public static int TIMEOUT = 3;

	private Map<Integer, InChannel> inConnections;
	private Map<Integer, OutChannel> outConnections;
	private RIONode n;

	private Map<Integer, Queue<outstandingSendRequest>> waitingMessages;

	public class outstandingSendRequest{
		public int destAddr;
		public int protocol;
		public byte[] payload;

		public outstandingSendRequest(int destAddr, int protocol, byte[] payload){
			this.destAddr = destAddr;
			this.protocol = protocol;
			this.payload = payload;
		}
	}

	/**
	 * Constructor.
	 * 
	 * @param destAddr
	 *            The address of the destination host
	 * @param msg
	 *            The message that was sent
	 * @param timeSent
	 *            The time that the ping was sent
	 */
	public ReliableInOrderMsgLayer(RIONode n) {
		inConnections = new HashMap<Integer, InChannel>();
		outConnections = new HashMap<Integer, OutChannel>();
		this.n = n;

		waitingMessages = new HashMap<Integer, Queue<outstandingSendRequest>>();
	}

	/**
	 * Receive a data packet.
	 * 
	 * @param from
	 *            The address from which the data packet came
	 * @param pkt
	 *            The Packet of data
	 */
	public void RIODataReceive(int from, byte[] msg) {
		RIOPacket riopkt = RIOPacket.unpack(msg);

		//int protocol = riopkt.getProtocol();

		InChannel in = inConnections.get(from);
		if(in == null){
			System.out.println("RIO RECEIVE: IN == NULL");
			
			resetConnection(from);
			
			//Never seen this connection before - Node recovering from failure
			byte[] seqNumByteArray = Utility.stringToByteArray("" + riopkt.getSeqNum());
			//Don't sent message ACK - respond with SESSION_START request
			n.send(from, Protocol.SESSION_START, seqNumByteArray);
		} else {
			System.out.println("RIO RECEIVE: SESSION STARTED");
			//Session started already

			// at-most-once semantics
			byte[] seqNumByteArray = Utility.stringToByteArray("" + riopkt.getSeqNum());
			n.send(from, Protocol.ACK, seqNumByteArray);

			LinkedList<RIOPacket> toBeDelivered = in.gotPacket(riopkt);
			for(RIOPacket p: toBeDelivered) {
				// deliver in-order the next sequence of packets
				n.onRIOReceive(from, p.getProtocol(), p.getPayload());
			}
		}
	}
	
	public void RIOSessionStartReceive(int from, byte[] msg) {
		//RIOPacket riopkt = RIOPacket.unpack(msg);		
		System.out.println("RIOSessionStartReceive from "+from);
		//Other node trying to establish a connection
		resetConnection(from);

		// at-most-once semantics
		//byte[] seqNumByteArray = Utility.stringToByteArray("" + riopkt.getSeqNum());
		n.send(from, Protocol.SESSION_START_ACK, new byte[0]);
	}
	
	public void RIOSessionStartAckReceive(int from, byte[] msg) {
		System.out.println("RIOSessionStartAckReceive from "+from);
		Queue<outstandingSendRequest> requests = waitingMessages.get(from);
		if(requests != null){
			OutChannel out = outConnections.get(from);

			for(outstandingSendRequest o : requests){
				//n.send(o.destAddr, o.protocol, o.payload);
				out.sendRIOPacket(n, o.protocol, o.payload);
			}
			waitingMessages.remove(from);
		}
	}
	
	private void resetConnection(int node) {
		InChannel in = new InChannel();
		OutChannel out = new OutChannel(this, node);
		//Other node trying to establish a connection
		
		inConnections.put(node, in);
		outConnections.put(node, out);
	}

	/**
	 * Receive an acknowledgment packet.
	 * 
	 * @param from
	 *            The address from which the data packet came
	 * @param pkt
	 *            The Packet of data
	 */
	public void RIOAckReceive(int from, byte[] msg) {
		int seqNum = Integer.parseInt( Utility.byteArrayToString(msg) );
		outConnections.get(from).gotACK(seqNum);
	}

	/**
	 * Send a packet using this reliable, in-order messaging layer. Note that
	 * this method does not include a reliable, in-order broadcast mechanism.
	 * 
	 * @param destAddr
	 *            The address of the destination for this packet
	 * @param protocol
	 *            The protocol identifier for the packet
	 * @param payload
	 *            The payload to be sent
	 */
	public void RIOSend(int destAddr, int protocol, byte[] payload) {
		OutChannel out = outConnections.get(destAddr);
		if(out == null) {
			System.out.println("RIO SEND: out == null");
			//Never sent to this connection before -- start session
			resetConnection(destAddr);

			Queue<outstandingSendRequest> requests = waitingMessages.get(destAddr);
			if(requests == null){
				requests = new LinkedList<outstandingSendRequest>();
				waitingMessages.put(destAddr, requests);
			}
			requests.add(new outstandingSendRequest(destAddr, protocol, payload));

			n.send(destAddr, Protocol.SESSION_START, new byte[0]);
		} else if (waitingMessages.containsKey(destAddr)) {
			System.out.println("RIO SEND: there are waiting messages");
			Queue<outstandingSendRequest> requests = waitingMessages.get(destAddr);
			requests.add(new outstandingSendRequest(destAddr, protocol, payload));
		} else {
			System.out.println("RIO SEND: out != null and session ACK'd");
			//Session already started -- send message
			out.sendRIOPacket(n, protocol, payload);
		}
	}

	/**
	 * Callback for timeouts while waiting for an ACK.
	 * 
	 * This method is here and not in OutChannel because OutChannel is not a
	 * public class.
	 * 
	 * @param destAddr
	 *            The receiving node of the unACKed packet
	 * @param seqNum
	 *            The sequence number of the unACKed packet
	 */
	public void onTimeout(Integer destAddr, Integer seqNum) {
		outConnections.get(destAddr).onTimeout(n, seqNum);
	}

	@Override
	public String toString() {
		StringBuffer sb = new StringBuffer();
		for(Integer i: inConnections.keySet()) {
			sb.append(inConnections.get(i).toString() + "\n");
		}

		return sb.toString();
	}
}

/**
 * Representation of an incoming channel to this node
 */
class InChannel {
	private int lastSeqNumDelivered;
	private HashMap<Integer, RIOPacket> outOfOrderMsgs;

	InChannel(){
		lastSeqNumDelivered = -1;
		outOfOrderMsgs = new HashMap<Integer, RIOPacket>();
	}

	/**
	 * Method called whenever we receive a data packet.
	 * 
	 * @param pkt
	 *            The packet
	 * @return A list of the packets that we can now deliver due to the receipt
	 *         of this packet
	 */
	public LinkedList<RIOPacket> gotPacket(RIOPacket pkt) {
		LinkedList<RIOPacket> pktsToBeDelivered = new LinkedList<RIOPacket>();
		int seqNum = pkt.getSeqNum();

		if(seqNum == lastSeqNumDelivered + 1) {
			// We were waiting for this packet
			pktsToBeDelivered.add(pkt);
			++lastSeqNumDelivered;
			deliverSequence(pktsToBeDelivered);
		}else if(seqNum > lastSeqNumDelivered + 1){
			// We received a subsequent packet and should store it
			outOfOrderMsgs.put(seqNum, pkt);
		}
		// Duplicate packets are ignored

		return pktsToBeDelivered;
	}

	/**
	 * Helper method to grab all the packets we can now deliver.
	 * 
	 * @param pktsToBeDelivered
	 *            List to append to
	 */
	private void deliverSequence(LinkedList<RIOPacket> pktsToBeDelivered) {
		while(outOfOrderMsgs.containsKey(lastSeqNumDelivered + 1)) {
			++lastSeqNumDelivered;
			pktsToBeDelivered.add(outOfOrderMsgs.remove(lastSeqNumDelivered));
		}
	}

	@Override
	public String toString() {
		return "last delivered: " + lastSeqNumDelivered + ", outstanding: " + outOfOrderMsgs.size();
	}
}

/**
 * Representation of an outgoing channel to this node
 */
class OutChannel {
	private HashMap<Integer, RIOPacket> unACKedPackets;
	private int lastSeqNumSent;
	private ReliableInOrderMsgLayer parent;
	private int destAddr;

	OutChannel(ReliableInOrderMsgLayer parent, int destAddr){
		lastSeqNumSent = -1;
		unACKedPackets = new HashMap<Integer, RIOPacket>();
		this.parent = parent;
		this.destAddr = destAddr;
	}

	/**
	 * Send a new RIOPacket out on this channel.
	 * 
	 * @param n
	 *            The sender and parent of this channel
	 * @param protocol
	 *            The protocol identifier of this packet
	 * @param payload
	 *            The payload to be sent
	 */
	protected void sendRIOPacket(RIONode n, int protocol, byte[] payload) {
		try{
			Method onTimeoutMethod = Callback.getMethod("onTimeout", parent, new String[]{ "java.lang.Integer", "java.lang.Integer" });
			RIOPacket newPkt = new RIOPacket(protocol, ++lastSeqNumSent, payload);
			unACKedPackets.put(lastSeqNumSent, newPkt);

			n.send(destAddr, Protocol.DATA, newPkt.pack());
			n.addTimeout(new Callback(onTimeoutMethod, parent, new Object[]{ destAddr, lastSeqNumSent }), ReliableInOrderMsgLayer.TIMEOUT);
		}catch(Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * Called when a timeout for this channel triggers
	 * 
	 * @param n
	 *            The sender and parent of this channel
	 * @param seqNum
	 *            The sequence number of the unACKed packet
	 */
	public void onTimeout(RIONode n, Integer seqNum) {
		if(unACKedPackets.containsKey(seqNum)) {
			resendRIOPacket(n, seqNum);
		}
	}

	/**
	 * Called when we get an ACK back. Removes the outstanding packet if it is
	 * still in unACKedPackets.
	 * 
	 * @param seqNum
	 *            The sequence number that was just ACKed
	 */
	protected void gotACK(int seqNum) {
		unACKedPackets.remove(seqNum);
	}

	/**
	 * Resend an unACKed packet.
	 * 
	 * @param n
	 *            The sender and parent of this channel
	 * @param seqNum
	 *            The sequence number of the unACKed packet
	 */
	private void resendRIOPacket(RIONode n, int seqNum) {
		try{
			Method onTimeoutMethod = Callback.getMethod("onTimeout", parent, new String[]{ "java.lang.Integer", "java.lang.Integer" });
			RIOPacket riopkt = unACKedPackets.get(seqNum);

			n.send(destAddr, Protocol.DATA, riopkt.pack());
			n.addTimeout(new Callback(onTimeoutMethod, parent, new Object[]{ destAddr, seqNum }), ReliableInOrderMsgLayer.TIMEOUT);
		}catch(Exception e) {
			e.printStackTrace();
		}
	}
}
