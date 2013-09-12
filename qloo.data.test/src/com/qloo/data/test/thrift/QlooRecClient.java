package com.qloo.data.test.thrift;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

import com.qloo.data.thrift.Choice;
import com.qloo.data.thrift.QlooRecService;
import com.qloo.data.thrift.User;
import com.qloo.data.util.CategoryUtil;
import com.qloo.data.util.CommandUtil;
import com.qloo.data.util.UUIDUtil;


public class QlooRecClient {
	
	protected static Options getOptions() {
		Options options = new Options();
		
		options.addOption("h", true, "Qloo Recommendation Server host");
		options.addOption("p", true, "port number");
		
		return options;
	}

	public static void main(String[] args) {
		CommandLine cmd = CommandUtil.parse(QlooRecClient.class.getName(), args, QlooRecClient.getOptions());
		if (cmd == null) return;
		
		try {
			// TTransport transport = new TSocket("localhost", 8080);
			// TTransport transport = new TSocket("107.22.130.1", 9090);
			// TTransport transport = new TSocket("174.129.63.56", 9090);
			
			TTransport transport = new TSocket(cmd.getOptionValue("h"), Integer.parseInt(cmd.getOptionValue("p")));

			TProtocol protocol = new TBinaryProtocol(transport);

			QlooRecService.Client client = new QlooRecService.Client(protocol);
			
			transport.open();
			
			Thread.sleep(1000);
			
			UUID uid = UUID.fromString("07E977E3-1BCA-4F4F-A8A9-F8D41DB097D2");
			//UUID uid = UUID.fromString("07E977E3-1BCA-4F4F-A8A9-F8D41DB097D0");
			//UUID uid = UUID.fromString("d66ff676-a885-4fd3-b296-ec565ed13a5f");
			//UUID uid = UUID.fromString("8be16f7a-d775-4345-b5d3-297bef9d6ce0");
			
			List<Byte> uidList = UUIDUtil.toByteList(uid);
			
			System.out.println("uid: " + UUIDUtil.uuid(client.echoUUID(uidList)));
			
			List<Short> categoryList = new ArrayList<Short>();
			categoryList.add((short)0);
			categoryList.add((short)1);
			categoryList.add((short)2);
			categoryList.add((short)3);
			categoryList.add((short)4);
			categoryList.add((short)5);
			categoryList.add((short)6);
			categoryList.add((short)7);

			short choiceOpt = 0;
			short categoryOpt = 0;
			short topicOpt = 14855;
			short dataOpt = 0;
			short pageOpt = 0;
			
			long lStart = System.currentTimeMillis();
			
			//Map<User, Double> uMap = client.getUserSimilarityMap2(uidList, (short)1, (short)0, dataOpt, pageOpt);
			Map<User, Double> uMap = client.getUserSimilarityMap2(uidList, (short)0, (short)CategoryUtil.toLong(categoryList), dataOpt, pageOpt);
			//Map<User, Double> uMap = client.getUserSimilarityMap3(uidList, (short)0, categoryOpt, dataOpt, pageOpt);
			
			//Map<User, Double> uMap = client.getUserSimilarityMap2(uidList, (short)CategoryUtil.toInteger(categoryList), (short)0, dataOpt, pageOpt);
			
			System.out.println("getUserSimilarityMap2_basic takes: " + (System.currentTimeMillis() - lStart) + " mill seconds");
			
			for (Map.Entry<User, Double> entry : uMap.entrySet()) {
				User u = entry.getKey();
				
				System.out.println("score: " + entry.getValue() + "\tuid: " + UUIDUtil.uuid(u.uid) + "\tgender: " + u.gender + "\tage: " + u.age);
			}
			
			lStart = System.currentTimeMillis();
			
			Map<Choice, Double> cMap = client.getChoiceRecommendationMap3(uidList, (short)0, choiceOpt, categoryOpt, topicOpt, dataOpt, pageOpt);
			
			System.out.println("getChoiceRecommendationMap3_basic takes: " + (System.currentTimeMillis() - lStart) + " mill seconds");
			
			for (Map.Entry<Choice, Double> entry : cMap.entrySet()) {
				Choice c = entry.getKey();
				
				System.out.println("score: " + entry.getValue() + "\tcid: " + UUIDUtil.uuid(c.cid) + "\tgid: " + c.gid);
			}
			
			
			long lStart2 = System.currentTimeMillis();
			
			Map<User, Double> uMap2 = client.getUserSimilarityMap(uidList, (short)1, dataOpt, pageOpt);
			
			System.out.println("getUserSimilarMap_cosine takes: " + (System.currentTimeMillis() - lStart2) + " mill seconds");
			
			for (Map.Entry<User, Double> entry : uMap2.entrySet()) {
				User u = entry.getKey();
				
				System.out.println("score: " + entry.getValue() + "\tuid: " + UUIDUtil.uuid(u.uid) + "\tgender: " + u.gender + "\tage: " + u.age);
			}
			
			lStart2 = System.currentTimeMillis();
			
			Map<Choice, Double> cMap2 = client.getChoiceRecommendationMap(uidList, (short)1, choiceOpt, categoryOpt, dataOpt, pageOpt);
			
			System.out.println("getChoiceRecommenderMap_cosine takes: " + (System.currentTimeMillis() - lStart2) + " mill seconds");
			
			for (Map.Entry<Choice, Double> entry : cMap2.entrySet()) {
				Choice c = entry.getKey();
				
				System.out.println("score: " + entry.getValue() + "\tcid: " + UUIDUtil.uuid(c.cid) + "\tgid: " + c.gid);
			}
			
			/*
			long lStart3 = System.currentTimeMillis();
			
			Map<User, Double> uMap3 = client.getUserSimilarityMap(uidList, (short)2, dataOpt, pageOpt);
			
			System.out.println("getUserSimilarMap_euclidean takes: " + (System.currentTimeMillis() - lStart3) + " mill seconds");
			
			for (Map.Entry<User, Double> entry : uMap3.entrySet()) {
				User u = entry.getKey();
				
				System.out.println("score: " + entry.getValue() + "\tuid: " + UUIDUtil.uuid(u.uid) + "\tgender: " + u.gender + "\tage: " + u.age);
			}
			
			lStart3 = System.currentTimeMillis();
			
			Map<Choice, Double> cMap3 = client.getChoiceRecommendationMap(uidList, (short)2, choiceOpt, dataOpt, pageOpt);
			
			System.out.println("getChoiceRecommenderMap_euclidean takes: " + (System.currentTimeMillis() - lStart3) + " mill seconds");
			
			for (Map.Entry<Choice, Double> entry : cMap3.entrySet()) {
				Choice c = entry.getKey();
				
				System.out.println("score: " + entry.getValue() + "\tcid: " + UUIDUtil.uuid(c.cid) + "\tgid: " + c.gid);
			}
			
			
			long lStart4 = System.currentTimeMillis();
			
			Map<User, Double> uMap4 = client.getUserSimilarityMap(uidList, (short)3, dataOpt, pageOpt);
			
			System.out.println("getUserSimilarMap_pearson takes: " + (System.currentTimeMillis() - lStart4) + " mill seconds");
			
			for (Map.Entry<User, Double> entry : uMap4.entrySet()) {
				User u = entry.getKey();
				
				System.out.println("score: " + entry.getValue() + "\tuid: " + UUIDUtil.uuid(u.uid) + "\tgender: " + u.gender + "\tage: " + u.age);
			}
			
			lStart3 = System.currentTimeMillis();
			
			Map<Choice, Double> cMap4 = client.getChoiceRecommendationMap(uidList, (short)3, choiceOpt, dataOpt, pageOpt);
			
			System.out.println("getChoiceRecommenderMap_pearson takes: " + (System.currentTimeMillis() - lStart4) + " mill seconds");
			
			for (Map.Entry<Choice, Double> entry : cMap4.entrySet()) {
				Choice c = entry.getKey();
				
				System.out.println("score: " + entry.getValue() + "\tcid: " + UUIDUtil.uuid(c.cid) + "\tgid: " + c.gid);
			}*/
			
			// System.out.println("closing: " + client.close());
			
			transport.close();

		} catch (TTransportException e) {
			e.printStackTrace();
		} catch (TException x) {
			x.printStackTrace();
		} catch (Throwable th) {
			th.printStackTrace();
		}
	}
}
