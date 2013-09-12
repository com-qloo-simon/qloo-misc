package com.qloo.data.test.dao.astyanax;

import org.junit.BeforeClass;
import org.junit.Test;

import com.qloo.data.cassandra.ChoiceDAO;
import com.qloo.data.cassandra.KSFactory;


public class ChoiceDAOTest {
	static ChoiceDAO cd;
	
	@BeforeClass
    public static void oneTimeSetUp() {
		System.out.println("@BeforeClass - oneTimeSetUp");
		
		cd = new ChoiceDAO();
	    cd.init(KSFactory.init("dse1", "107.22.7.122,54.242.215.222", "qloo_b3"), "choice_used");
    }
	
	@Test
	public void load() {
		System.out.println("@Test - load");
		
		cd.load(KSFactory.init("dse1", "107.22.7.122,54.242.215.222", "baldr"), "choice_used");
	}
}
