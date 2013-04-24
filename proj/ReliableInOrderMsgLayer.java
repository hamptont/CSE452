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

	private Map<Integer, Set<OutstandingSendRequest>> waitingMessages;
	private Set<Integer> awaitingSessionAck;

	private class OutstandingSendRequest{
		public int destAddr;
		public int protocol;
		public byte[] payload;

		public OutstandingSendRequest(int destAddr, int protocol, byte[] payload){
			this.destAddr = destAddr;
			this.protocol = protocol;
			this.payload = payload;
		}
		
		public int hashCode() {
			return destAddr ^ protocol ^ payload.hashCode();			
		}
		
		public boolean equals(Object o) {
			if(o == null || getClass() != o.getClass()) {
				return false;
			}
			
			OutstandingSendRequest other = (OutstandingSendRequest)o;
			
			return destAddr == other.destAddr && protocol == other.protocol && Arrays.equals(payload, other.payload);
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

		waitingMessages = new HashMap<Integer, Set<OutstandingSendRequest>>();
		awaitingSessionAck = new HashSet<Integer>();
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

		InChannel in = inConnections.get(from);
		if(in == null){
			System.out.println("RIO RECEIVE: connections not yet established with "+from);
			
			//Never seen this connection before - Node recovering from failure
			resetConnections(from);
			
			//Don't sent message ACK - respond with SESSION_START request
			sendSessionReqPacket(from);
		} else {
			System.out.println("RIO RECEIVE: normal operation");
			//Session started already

			// ACK the received packet
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
	
	/**
	 * Receive a session start request.
	 * 
	 * @param from
	 *            The address from which the data packet came
	 * @param pkt
	 *            The Packet of data
	 */
	public void RIOSessionStartReceive(int from, byte[] msg) {
		System.out.println("RIOSessionStartReceive from "+from);
		
		//Other node trying to establish a connection
		resetConnections(from);

		n.send(from, Protocol.SESSION_START_ACK, new byte[0]);
	}
	
	/**
	 * Receive a session start ack.
	 * 
	 * @param from
	 *            The address from which the data packet came
	 * @param pkt
	 *            The Packet of data
	 */
	public void RIOSessionStartAckReceive(int from, byte[] msg) {
		System.out.println("RIOSessionStartAckReceive from "+from);
		Set<OutstandingSendRequest> requests = waitingMessages.get(from);
		if(requests != null){
			OutChannel out = outConnections.get(from);

			for(OutstandingSendRequest o : requests){
				//n.send(o.destAddr, o.protocol, o.payload);
				out.sendRIOPacket(n, o.protocol, o.payload);
			}
			waitingMessages.remove(from);
		}
		awaitingSessionAck.remove(from);
	}
	
	// create new in and outChannels for the given node
	private void resetConnections(int node) {
		InChannel in = new InChannel();
		OutChannel out = new OutChannel(this, node);
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
			System.out.println("RIO SEND: connection not established, sending new session request");
			//Never sent to this connection before -- start session
			resetConnections(destAddr);

			// add the RIOSend request to the queue
			Set<OutstandingSendRequest> requests = waitingMessages.get(destAddr);
			if(requests == null){
				requests = new LinkedHashSet<OutstandingSendRequest>();
				waitingMessages.put(destAddr, requests);
			}
			requests.add(new OutstandingSendRequest(destAddr, protocol, payload));

			sendSessionReqPacket(destAddr);			
		} else if (waitingMessages.containsKey(destAddr)) {
			System.out.println("RIO SEND: queueing new message while awaiting session start ack");
			Set<OutstandingSendRequest> requests = waitingMessages.get(destAddr);
			requests.add(new OutstandingSendRequest(destAddr, protocol, payload));
		} else {
			System.out.println("RIO SEND: normal operation");
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
	
	/**
	 * Called when a timeout for this channel triggers
	 * 
	 * @param n
	 *            The sender and parent of this channel
	 * @param seqNum
	 *            The sequence number of the unACKed packet
	 */
	public void sessionReqTimeout(Integer destAddr) {
		if(awaitingSessionAck.contains(destAddr)) {
			sendSessionReqPacket(destAddr);
		}
		
	}

	/**
	 * Resend an unACKed Session request packet.
	 * 
	 * @param n
	 *            The sender and parent of this channel
	 * @param seqNum
	 *            The sequence number of the unACKed packet
	 */
	private void sendSessionReqPacket(Integer destAddr) {
		try{
			Method onTimeoutMethod = Callback.getMethod("sessionReqTimeout", this, new String[]{ "java.lang.Integer"});

			n.send(destAddr, Protocol.SESSION_START, new byte[0]);
			awaitingSessionAck.add(destAddr);
			
			n.addTimeout(new Callback(onTimeoutMethod, this, new Object[]{ destAddr }), ReliableInOrderMsgLayer.TIMEOUT);
		}catch(Exception e) {
			e.printStackTrace();
		}
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
