package com.qloo.data.test.thrift;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.UUID;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.*;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.netflix.astyanax.Keyspace;
import com.qloo.data.cassandra.ChoiceDAO;
import com.qloo.data.cassandra.KSFactory;
import com.qloo.data.cassandra.ProfileDAO;
import com.qloo.data.thrift.Choice;
import com.qloo.data.thrift.QlooRecService;
import com.qloo.data.thrift.User;
import com.qloo.data.util.CategoryUtil;
import com.qloo.data.util.CommandUtil;
import com.qloo.data.util.UUIDUtil;


public class QlooRecRobot implements Runnable {
	private static Logger logger = LoggerFactory.getLogger(QlooRecRobot.class);
	
	public final static long PERIOD = 1000 * 10;   // 10 seconds
	
	protected static Options getOptions() {
		Options options = new Options();
		
		options.addOption("i", true, "Cassandra seed ip address");
		options.addOption("h", true, "Qloo Recommendation Server host");
		options.addOption("p", true, "port number");
		options.addOption("d", true, "data option");
		
		return options;
	}

	static QlooRecService.Client client;
	static TTransport transport;
	
	static ProfileDAO pd = new ProfileDAO();
	static ChoiceDAO cd = new ChoiceDAO();
	
	static UUID[] uidArray;
	
	static List<Short> categoryList = new ArrayList<Short>();
	static {
		categoryList.add((short)0);
		categoryList.add((short)1);
		categoryList.add((short)2);
		categoryList.add((short)3);
		categoryList.add((short)4);
		categoryList.add((short)5);
		categoryList.add((short)6);
		categoryList.add((short)7);
	}
	static long categoryListOpt = CategoryUtil.toLong(categoryList);
	
	static short choiceOpt = 0;
	static short categoryOpt = 0;
	static short topicOpt = 14855;
	static short dataOpt = 1;
	static short pageOpt = 0;

	
	public void run() {
		while (true) {
			try {
				// logger.info("uid: " + UUIDUtil.uuid(client.echoUUID(uidList)));
				
				Random rand = new Random(System.currentTimeMillis());
				
				UUID uid = uidArray[rand.nextInt(uidArray.length)];
				
				List<Byte> uidList = UUIDUtil.toByteList(uid);
				
				long lStart = System.currentTimeMillis();
				
				//Map<User, Double> uMap = client.getUserSimilarityMap2(uidList, (short)1, (short)0, dataOpt, pageOpt);
				Map<User, Double> uMap = client.getUserSimilarityMap2(uidList, (short)0, categoryListOpt, dataOpt, pageOpt);
				//Map<User, Double> uMap = client.getUserSimilarityMap3(uidList, (short)0, categoryOpt, dataOpt, pageOpt);
				
				//Map<User, Double> uMap = client.getUserSimilarityMap2(uidList, (short)CategoryUtil.toInteger(categoryList), (short)0, dataOpt, pageOpt);
				
				logger.info("basic similarity of uid: " + uid + " gets " + uMap.size() + " users and takes: " + (System.currentTimeMillis() - lStart) + " mill seconds");
				
				for (Map.Entry<User, Double> entry : uMap.entrySet()) {
					User u = entry.getKey();
					
					//logger.info("score: " + entry.getValue() + "\tuid: " + UUIDUtil.uuid(u.uid) + "\tgender: " + u.gender + "\tage: " + u.age);
				}
				
				lStart = System.currentTimeMillis();
				
				Map<Choice, Double> cMap = client.getChoiceRecommendationMap3(uidList, (short)0, choiceOpt, categoryOpt, topicOpt, dataOpt, pageOpt);
				
				logger.info("basic recommendation of uid: " + uid + " gets " + cMap.size() + " choices and takes: " + (System.currentTimeMillis() - lStart) + " mill seconds");
				
				for (Map.Entry<Choice, Double> entry : cMap.entrySet()) {
					Choice c = entry.getKey();
					
					//logger.info("score: " + entry.getValue() + "\tcid: " + UUIDUtil.uuid(c.cid) + "\tgid: " + c.gid);
				}
			} catch (TTransportException e) {
				logger.error(e.getMessage(), e);
				if (transport != null) transport.close();
				setupClient();
			} catch (TException x) {
				logger.error(x.getMessage(), x);
				if (transport != null) transport.close();
				setupClient();
			} catch (Throwable th) {
				logger.error(th.getMessage(), th);
			}
			
			try {
				Thread.sleep(PERIOD);
			} catch (InterruptedException ie) {
				logger.error(ie.getMessage(), ie);
			}
		}
	}
	
	private static void setupClient() {
		try {			
			TSocket socket = new TSocket(cmd.getOptionValue("h"), Integer.parseInt(cmd.getOptionValue("p")));
			
			transport = new TFramedTransport(socket);
			
			transport.open();

			TProtocol protocol = new TBinaryProtocol(transport);

			client = new QlooRecService.Client(protocol);
		} catch (TTransportException e) {
			e.printStackTrace();
		} 
	}
	
	
	static CommandLine cmd;

	public static void main(String[] args) {
		cmd = CommandUtil.parse(QlooRecRobot.class.getName(), args, QlooRecRobot.getOptions());
		if (cmd == null) return;
		
		try {
			dataOpt = Short.parseShort(cmd.getOptionValue("d"));
			
			//Keyspace ks = KSFactory.init("dse1", "107.22.7.122,54.242.215.222", "qloo_b3");
			Keyspace ks = KSFactory.init("dse1", cmd.getOptionValue("i"), "qloo_b3");

		    pd.init(ks, "profile");
		    cd.init(ks, "choice_used");

		    Set<UUID> uidSet = pd.readAllUserInfoMap().keySet();
		    
		    uidArray = uidSet.toArray(new UUID[] {});
		    
		    logger.info("user count: " + uidArray.length);
		} catch (Throwable th) {
			th.printStackTrace();
			return;
		}
		
		setupClient();

		try {
			Thread.sleep(1000);
			new Thread(new QlooRecRobot()).start();
		} catch (Throwable th) {
			th.printStackTrace();
		}
	}
}
