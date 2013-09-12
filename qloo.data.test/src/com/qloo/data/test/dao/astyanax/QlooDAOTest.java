package com.qloo.data.test.dao.astyanax;

import java.util.Date;
import java.util.HashMap;
import java.util.UUID;

import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import com.qloo.data.cassandra.KSFactory;
import com.qloo.data.cassandra.QlooDAO;
import com.qloo.data.util.DateUtil;


public class QlooDAOTest {
	static QlooDAO qd;
	
	static UUID uid;
	
	@BeforeClass
    public static void oneTimeSetUp() {
		System.out.println("@BeforeClass - oneTimeSetUp");
		
		qd = new QlooDAO();
	    qd.init(KSFactory.init("dse1", "107.22.7.122,54.242.215.222", "qloo_b3"), "qloo");
	    
		uid = UUID.fromString("07E977E3-1BCA-4F4F-A8A9-F8D41DB097D2");
		// uid = UUID.fromString("e32b8d38-7674-4bde-959a-003f05c135bc".toUpperCase());
    }
	
	@Ignore("Not Ready to Run")
	@Test
	public void load() {
		System.out.println("@Test - load");

	    qd.load(KSFactory.init("dse1", "107.22.7.122,54.242.215.222", "baldr"), "qloo");
	}
	
	@Test
	public void test() {
		System.out.println("@Test - test");
		
		HashMap<String, Object> qlooMap = qd.readRow(uid, new String[] {"cid"},
	    		new int[] {qd.COLUMN_TYPE_UUID});
	    
		/*
		HashMap<String, Object> qlooMap = qd.readRow(uid, new String[] {"cid", "giid", "tiid", "gender", "dob"},
	    		new int[] {qd.COLUMN_TYPE_UUID, qd.COLUMN_TYPE_INT, qd.COLUMN_TYPE_INT,
					qd.COLUMN_TYPE_BOOLEAN, qd.COLUMN_TYPE_TIMESTAMP});*/
		
		/*
	    qd.readAllRows(
	    		new String[] {"uid", "cid", "giid", "tiid", "tug", "gender", "dob", "ether", "lat", "lon", "ctid"},
	    		new int[] {qd.COLUMN_TYPE_UUID, qd.COLUMN_TYPE_UUID, 
	    				qd.COLUMN_TYPE_INT, qd.COLUMN_TYPE_INT, qd.COLUMN_TYPE_BOOLEAN, 
	    				qd.COLUMN_TYPE_BOOLEAN, qd.COLUMN_TYPE_TIMESTAMP, 
	    				qd.COLUMN_TYPE_BOOLEAN,
	    				qd.COLUMN_TYPE_DOUBLE, qd.COLUMN_TYPE_DOUBLE, qd.COLUMN_TYPE_INT},
	    		false);*/
		
		System.out.println("cid: " + (UUID)qlooMap.get("cid") + "\tgender: " + (Boolean)qlooMap.get("gender") + "\tage: " + DateUtil.getAge(((Date)qlooMap.get("dob")).getTime()));
	}
}
