package com.chinabank.wyrcm;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

import javax.security.sasl.SaslException;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hive.service.auth.PlainSaslHelper;
import org.apache.hive.service.cli.thrift.TCLIService;
import org.apache.hive.service.cli.thrift.TCloseOperationReq;
import org.apache.hive.service.cli.thrift.TCloseSessionReq;
import org.apache.hive.service.cli.thrift.TExecuteStatementReq;
import org.apache.hive.service.cli.thrift.TExecuteStatementResp;
import org.apache.hive.service.cli.thrift.TFetchOrientation;
import org.apache.hive.service.cli.thrift.TFetchResultsReq;
import org.apache.hive.service.cli.thrift.TFetchResultsResp;
import org.apache.hive.service.cli.thrift.TOpenSessionReq;
import org.apache.hive.service.cli.thrift.TOpenSessionResp;
import org.apache.hive.service.cli.thrift.TOperationHandle;
import org.apache.hive.service.cli.thrift.TRow;
import org.apache.hive.service.cli.thrift.TRowSet;
import org.apache.hive.service.cli.thrift.TSessionHandle;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.mortbay.log.Log;

public class TestThrift {
	public static boolean deleteTable(String table_name, String sql) {
		try {
			Class.forName("org.apache.hive.jdbc.HiveDriver");
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
			System.exit(1);
		}
		try {
			Connection con = DriverManager.getConnection(
					"jdbc:hive2://172.17.59.20:10000/tmp", "root", "");
			Statement stmt = con.createStatement();
			Log.info(sql);
			stmt.executeQuery("use tmp");
			stmt.executeQuery(sql);
			// stmt.executeQuery("use tmp truncate table " + table_name);
			Log.info("exec state complete!");
		} catch (SQLException e) {
			e.printStackTrace();
			return false;
		}
		return true;
	}

	public static boolean deleteTableThrift(String table_name, String sql, int port) throws SaslException, TTransportException {
		TTransport transport = PlainSaslHelper.getPlainTransport("wangke", "China@Bank123", new TSocket("172.17.59.20", port));
	    TProtocol protocol = new TBinaryProtocol(transport);
	    TCLIService.Client client = new TCLIService.Client(protocol);
		try {
			transport.open();
			TOpenSessionReq openReq = new TOpenSessionReq();
			TSessionHandle sessHandle = client.OpenSession(openReq).getSessionHandle();
			TExecuteStatementReq execReq = new TExecuteStatementReq(sessHandle,
					"use tmp");
			execReq.setRunAsync(false);
			TExecuteStatementReq execReq2 = new TExecuteStatementReq(
					sessHandle, "truncate table " + table_name);
			execReq2.setRunAsync(false);
			client.ExecuteStatement(execReq);
			client.ExecuteStatement(execReq2);

			TCloseSessionReq closeConnectionReq = new TCloseSessionReq(
					sessHandle);
			client.CloseSession(closeConnectionReq);
			transport.close();
		} catch (TTransportException e1) {
			e1.printStackTrace();
			return false;
		} catch (TException e) {
			e.printStackTrace();
			return false;
		}
		return true;
	}

	public static void main(String[] args) throws ParseException, SaslException, TTransportException {
		// create the command line parser
		CommandLineParser parser = new GnuParser();
		Options options = new Options();
		options.addOption("h", "help", false, "print help .");
		options.addOption("q", "sql", true, "sql command.");
		options.addOption("t", "table", true, "sql table.");
		options.addOption("p", "port", true, "meta thrift port.");
		HelpFormatter formatter = new HelpFormatter();
		CommandLine line = parser.parse(options, args);
		if (line.hasOption('h') || line.hasOption("help")) {
			formatter.printHelp("TransactionStatistic", options, true);
			return;
		}
		String table = line.getOptionValue("table");
		String sql = line.getOptionValue("sql");
		int port = Integer.parseInt(line.getOptionValue("port"));
		deleteTableThrift(table, sql, port);
	}

}
