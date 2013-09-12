package com.qloo.data.test.thrift;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.thrift.TException;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TServerTransport;
import org.apache.thrift.server.TServer;
//import org.apache.thrift.server.TServer.Args;
//import org.apache.thrift.server.TSimpleServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.qloo.data.util.CommandUtil;


public class QlooGraphServer {
	private static Logger logger = LoggerFactory.getLogger(QlooGraphServer.class);

	public static void start(final QlooGraphService.Processor<QlooGraphServiceHandler> processor, final int port) {
		try {
			TServerTransport serverTransport = new TServerSocket(port);

			// TServer server = new TSimpleServer(new Args(serverTransport).processor(processor));

			TServer server = new TThreadPoolServer(new TThreadPoolServer.Args(serverTransport).processor(processor));

			System.out.println("Starting the Qloo Graph Server...");

			server.serve();
		} catch (TException te) {
			logger.error(te.getMessage(), te);
		}
	}
	
	protected static Options getOptions() {
		Options options = new Options();
		
		options.addOption("i", true, "Qloo Graph 3 input file");
		options.addOption("p", true, "port number");
		
		return options;
	}

	public static void main(String[] args) {
		CommandLine cmd = CommandUtil.parse(QlooGraphServer.class.getName(), args, QlooGraphServer.getOptions());
		if (cmd == null) return;
		
		start(new QlooGraphService.Processor<QlooGraphServiceHandler>(new QlooGraphServiceHandler(cmd.getOptionValue("i"))), Integer.parseInt(cmd.getOptionValue("p")));
	}
}