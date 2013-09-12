package com.qloo.data.test.dao.astyanax;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import com.qloo.data.cassandra.KSFactory;
import com.qloo.data.cassandra.ProfileDAO;
import com.qloo.data.util.DateUtil;


public class ProfileDAOTest {
	static ProfileDAO pd;
	
	static UUID uid;
	
	
	@BeforeClass
    public static void oneTimeSetUp() {
		System.out.println("@BeforeClass - oneTimeSetUp");
		
		pd = new ProfileDAO();
	    pd.init(KSFactory.init("dse1", "107.22.7.122,54.242.215.222", "qloo_b3"), "profile");
	    
		// uid = UUID.fromString("07E977E3-1BCA-4F4F-A8A9-F8D41DB097D2");
		uid = UUID.fromString("e32b8d38-7674-4bde-959a-003f05c135bc".toUpperCase());
    }
	
	@Ignore("Not Ready to Run")
	@Test
	public void load() {
		System.out.println("@Test - load");
		
		pd.load(KSFactory.init("dse1", "107.22.7.122,54.242.215.222", "rds1"), "profile");
	}
	
	@Ignore("Not Ready to Run")
	@Test
	public void printAllUserInfoMap() {
		System.out.println("@Test - printAllUserInfoMap");
		
		Map<UUID, ProfileDAO.UserInfo> map = pd.readAllUserInfoMap();

		for (Map.Entry<UUID, ProfileDAO.UserInfo> entry : map.entrySet()) {
	    	System.out.println("id: " + entry.getKey() + "\tuname: " + entry.getValue().uname + "\tgender: " + entry.getValue().gender + "\tdob: " + entry.getValue().dob);
	    }
	}
	
	@Test
	public void printUserInfoMap() {
		System.out.println("@Test - printUserInfoMap");
		
		HashMap<String, Object> userMap = pd.readRow(uid, new String[] {"name", "gender", "dob"},
	    		new int[] {pd.COLUMN_TYPE_STRING, pd.COLUMN_TYPE_BOOLEAN, pd.COLUMN_TYPE_TIMESTAMP});
		
		System.out.println("name: " + userMap.get("name") + "\tgender: " + (Boolean)userMap.get("gender") + "\tage: " + DateUtil.getAge(((Date)userMap.get("dob")).getTime()));
	}
}
