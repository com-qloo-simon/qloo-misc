package com.qloo.data.test.thrift;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.Ignore;

import com.qloo.data.thrift.Choice;
import com.qloo.data.thrift.QlooRecService;
import com.qloo.data.thrift.User;
import com.qloo.data.thrift.Geo;
import com.qloo.data.util.CategoryUtil;
import com.qloo.data.util.UUIDUtil;


public class QlooRecClientTest {
	static TTransport transport;
	static QlooRecService.Client client;
	
	static UUID uid;
	static List<Byte> uidList;
	
	static short userOpt = 0;
	static short choiceOpt = 0;
	
	static List<Short> categoryList = new ArrayList<Short>();
	static long categoryListOpt = 0;
	static short categoryOpt = 0;
	
	static short topicOpt = 14855;
	static short dataOpt = 1;
	static short pageOpt = 0;

	static Geo geo = new Geo();

	
	@BeforeClass
    public static void oneTimeSetUp() {
		System.out.println("@BeforeClass - oneTimeSetUp");
		
		try {			
			TSocket socket = new TSocket("174.129.63.56", 9090);
			// transport = new TSocket("107.20.43.35", 9090);
			// TSocket socket = new TSocket("localhost", 9090);
			
			transport = new TFramedTransport(socket);
			
			transport.open();

			TProtocol protocol = new TBinaryProtocol(transport);

			client = new QlooRecService.Client(protocol);
		} catch (TTransportException e) {
			e.printStackTrace();
		}
		
		// uid = UUID.fromString("8be16f7a-d775-4345-b5d3-297bef9d6ce0");
		// uid = UUID.fromString("07E977E3-1BCA-4F4F-A8A9-F8D41DB097D2");
		// uid = UUID.fromString("E32B8D38-7674-4BDE-959A-003F05C135BC");
		uid = UUID.fromString("e32b8d38-7674-4bde-959a-003f05c135bc");
		
		uidList = UUIDUtil.toByteList(uid);
		
		categoryList.add((short)0);
		categoryList.add((short)1);
		categoryList.add((short)2);
		categoryList.add((short)3);
		categoryList.add((short)4);
		categoryList.add((short)5);
		categoryList.add((short)6);
		categoryList.add((short)7);
		
		categoryListOpt = CategoryUtil.toLong(categoryList);
		
		geo.cityId = 2;
		geo.lon = -73.993610;
		geo.lat = 40.720915;
    }
	
	//@Ignore("Not Ready to Run")
    @AfterClass
    public static void oneTimeTearDown() {
    	System.out.println("@AfterClass - oneTimeTearDown");
    	
    	if (transport != null) {
    		transport.close();
    	}
    }
    
	//@Ignore("Not Ready to Run")
	@Test
	public void echoUUID() {
		System.out.println("@Test - echoUUID");
		
		try {
			System.out.println("echo > " + UUIDUtil.uuid(client.echoUUID(uidList)));
		} catch (TException te) {
			te.printStackTrace();
		}
	}
	
	//@Ignore("Not Ready to Run")
	@Test
	public void monitorJVM() {
		System.out.println("@Test - monitorJVM");
		
		try {
			System.out.println("thread count: " + client.getThreadCount());
			System.out.println("memory usage: " + client.getMemoryUsage() + " MB");
		} catch (TException te) {
			te.printStackTrace();
		}
	}
	
	//@Ignore("Not Ready to Run")
	@Test
	public void printNeighborCount() {
		System.out.println("@Test - printNeighborCount");
		
		try {
			System.out.println("neighbor count: " + client.getNeighborCount(uidList));
		} catch (TException te) {
			te.printStackTrace();
		}
	}
	
	//@Ignore("Not Ready to Run")
	@Test
	public void printUserSimilarityMap_basic() {
		System.out.println("@Test - printUserSimilarityMap_basic");
		
		userOpt = 0;  // basic
		
		try {
			long lStart = System.currentTimeMillis();
			
			Map<User, Double> uMap = client.getUserSimilarityMap(uidList, userOpt, dataOpt, pageOpt);
			
			//System.out.println("categoryListOpt: " + categoryListOpt);
			//Map<User, Double> uMap = client.getUserSimilarityMap2(uidList, userOpt, categoryListOpt, dataOpt, pageOpt);
			
			//Map<User, Double> uMap = client.getUserSimilarityMap3(uidList, userOpt, categoryOpt, dataOpt, pageOpt);
			
			System.out.println("uMap size: " + uMap.size() + " takes " + (System.currentTimeMillis() - lStart) + " mill seconds");
			
			for (Map.Entry<User, Double> entry : uMap.entrySet()) {
				User u = entry.getKey();
				
				System.out.println("score: " + entry.getValue() + "\tuid: " + UUIDUtil.uuid(u.uid) + "\tgender: " + u.gender + "\tage: " + u.age);
			}
		} catch (TException te) {
			te.printStackTrace();
		}
	}
	
	//@Ignore("Not Ready to Run")
	@Test
	public void printRecommendationMap_basic() {
		System.out.println("@Test - printRecommendationMap_basic");
		
		List<Byte> uidList = UUIDUtil.toByteList(uid);
		
		try {
			long lStart = System.currentTimeMillis();
			
			Map<Choice, Double> cMap = client.getChoiceRecommendationMap3(uidList, (short)0, choiceOpt, categoryOpt, topicOpt, dataOpt, pageOpt);
			
			//Map<Choice, Double> cMap = client.getChoiceRecommendationMap5(uidList, (short)0, choiceOpt, categoryOpt, topicOpt, dataOpt, geo);
			
			System.out.println("cMap size: " + cMap.size() + " takes " + (System.currentTimeMillis() - lStart) + " mill seconds");
			
			for (Map.Entry<Choice, Double> entry : cMap.entrySet()) {
				Choice c = entry.getKey();
				
				System.out.println("score: " + entry.getValue() + "\tcid: " + UUIDUtil.uuid(c.cid) + "\tgid: " + c.gid);
			}
		} catch (TException te) {
			te.printStackTrace();
		}
	}
}
