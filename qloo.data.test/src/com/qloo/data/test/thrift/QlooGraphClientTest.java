package com.qloo.data.test.thrift;

import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.Ignore;

import com.qloo.data.thrift.QlooGraphService;
import com.qloo.data.thrift.User;
import com.qloo.data.util.UUIDUtil;


public class QlooGraphClientTest {
	static TTransport transport;
	static QlooGraphService.Client client;
	
	@BeforeClass
    public static void oneTimeSetUp() {
		System.out.println("@BeforeClass - oneTimeSetUp");
		
		try {
			transport = new TSocket("localhost", 9080);
	
			TProtocol protocol = new TBinaryProtocol(transport);
	
			client = new QlooGraphService.Client(protocol);
			
			transport.open();
		} catch (TTransportException e) {
			e.printStackTrace();
		}
    }
    
	//@Ignore("Not Ready to Run")
	@Test
	public void echoUUID() {
		System.out.println("@Test - echoUUID");

		UUID uid = UUID.fromString("07E977E3-1BCA-4F4F-A8A9-F8D41DB097D2");
		
		List<Byte> uidList = UUIDUtil.toByteList(uid);
		
		try {
			System.out.println("echo > " + UUIDUtil.uuid(client.echoUUID(uidList)));
		} catch (TException te) {
			te.printStackTrace();
		}
	}
	
	@Test
	public void printNeighborCount() {
		System.out.println("@Test - printNeighborCount");

		UUID uid = UUID.fromString("07E977E3-1BCA-4F4F-A8A9-F8D41DB097D2");
		
		List<Byte> uidList = UUIDUtil.toByteList(uid);
		
		try {
			System.out.println("neighbor count: " + client.getNeighborCount(uidList));
		} catch (TException te) {
			te.printStackTrace();
		}
	}
}
