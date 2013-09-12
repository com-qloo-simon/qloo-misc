package com.qloo.data.test.thrift;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.thrift.TException;

import com.qloo.data.graph.netflix.QlooGraph3;
import com.qloo.data.util.UUIDUtil;


public class QlooGraphServiceHandler implements QlooGraphService.Iface {
    private static Logger logger = LoggerFactory.getLogger(QlooGraphServiceHandler.class);
    
    QlooGraph3 qg3;
    
    public QlooGraphServiceHandler(String dataFile) {
            qg3 = QlooGraph3.getInstance(dataFile);
    }
    
    @Deprecated
    @Override
    public List<Byte> echoUUID(List<Byte> uuid) throws TException {         
            return uuid;
    }

    public int getNeighborCount(List<Byte> userID) throws TException {
    	return qg3.getNeighborCount(UUIDUtil.uuid(userID));
    }
}
