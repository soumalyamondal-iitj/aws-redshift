package org.iitj.aws.redshift;

import java.io.File;
import java.io.FileNotFoundException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Scanner;

public class AmazonRedshift {

	private Connection con;

	// Fill in AWS connection information
	private String url = "jdbc:redshift://redshift-cluster-1.ckudvww7yfrc.ap-south-1.redshift.amazonaws.com:5439/dev";
	private String uid = "awsuser"; 
	private String pw = "Abc12345";

	public static void main(String[] args) throws SQLException {
		AmazonRedshift q = new AmazonRedshift();
		q.connect();
		
		//q.drop();		
		//q.create();		
		//q.insert();
		
		q.query1();
		q.query2();
		q.query3();
		
		q.close();
		
	}

	public Connection connect() throws SQLException {
		System.out.println("Connecting to database.");
		con = DriverManager.getConnection(url, uid, pw);
		System.out.println("Connecting to database done.");
		return con;
	}

	public void close() {
		System.out.println("Closing database connection.");
		try {
			if (con != null && !con.isClosed()) {
				con.close();
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	public void drop() {
	    System.out.println("Dropping all the tables");
	    String[] tables = {"lineitem", "orders", "partsupp", "part",
	"supplier", "customer", "nation", "region"};
	    for (String table : tables) {
	        try (Statement stmt = con.createStatement()) {
	            stmt.executeUpdate("DROP TABLE IF EXISTS dev." + table);
	        } catch (SQLException e) {
	            e.printStackTrace();
	        }
	    }
	}

	public void create() throws SQLException {
	    System.out.println("Creating Tables");
	    
	    String schema = "dev";
	    
	    try (Statement stmt = con.createStatement()) {
	        // Create schema if it does not exist
	        stmt.executeUpdate("CREATE SCHEMA IF NOT EXISTS " + schema);
	    }
	    
	    String[] ddlFiles = {
	        "tpch_create.sql"
	    };

	    for (String ddlFile : ddlFiles) {
	        executeSQLFile("C:\\temp\\DDL data\\"+ddlFile);
	    }
	}
	private void executeSQLFile(String filePath) throws SQLException {
	    try (Scanner scanner = new Scanner(new File(filePath))) {
	        scanner.useDelimiter(";");
	        while (scanner.hasNext()) {
	            String sqlStatement = scanner.next().trim();
	            if (!sqlStatement.isEmpty()) {
	                try (Statement stmt = con.createStatement()) {
	                    stmt.executeUpdate(sqlStatement);
	                }
	            }
	        }
	    } catch (FileNotFoundException e) {
	        e.printStackTrace();
	    }
	}

	public void insert() throws SQLException {
	    System.out.println("Loading TPC-H Data");
	    String[] dataFiles = {
	        "region.sql", "nation.sql", "supplier.sql",
	        "part.sql", "partsupp.sql", "customer.sql",
	        "orders.sql", "lineitem.sql"
	    };

	    for (String dataFile : dataFiles) {
	        executeSQLFile("C:\\temp\\DDL data\\"+dataFile);
	    }
	    System.out.println("Loading TPC-H Data - done");
	}

	public ResultSet query1() throws SQLException {
	    System.out.println("Executing query #1.");
	    String sql = "SELECT o_orderkey, o_totalprice, o_orderdate " +
	                 "FROM dev.orders " +
	                 "JOIN dev.customer ON o_custkey = c_custkey " +
	                 "JOIN dev.nation ON c_nationkey = n_nationkey " +
	                 "JOIN dev.region ON n_regionkey = r_regionkey " +
	                 "WHERE r_name = 'AMERICA' " +
	                 "ORDER BY o_orderdate DESC " +
	                 "LIMIT 10";
	    Statement stmt = con.createStatement();
	    ResultSet rs = stmt.executeQuery(sql);
	    System.out.println("Query #1 result: "+resultSetToString(rs, 100));
	    return rs;
	}

	public ResultSet query2() throws SQLException {
	    System.out.println("Executing query #2.");
	    String largestSegment = getLargestMarketSegment();

	    String sql = "SELECT c_custkey, SUM(o_totalprice) as total_price " +
	                 "FROM dev.orders " +
	                 "JOIN dev.customer ON o_custkey = c_custkey " +
	                 "JOIN dev.nation ON c_nationkey = n_nationkey " +
	                 "JOIN dev.region ON n_regionkey = r_regionkey " +
	                 "WHERE r_name != 'EUROPE' AND o_orderstatus != 'F' " +
	                 "AND o_orderpriority = '1-URGENT' " +
	                 "AND c_mktsegment = ? " +
	                 "GROUP BY c_custkey " +
	                 "ORDER BY total_price DESC";

	    PreparedStatement pstmt = con.prepareStatement(sql);
	    pstmt.setString(1, largestSegment);
	    ResultSet rs = pstmt.executeQuery();
	    System.out.println("Query #2 result: "+resultSetToString(rs, 500));
	    return rs;

	}
	
	private String getLargestMarketSegment() throws SQLException {
	    String sql = "SELECT c_mktsegment, COUNT(*) as count " +
	                 "FROM dev.customer " +
	                 "GROUP BY c_mktsegment " +
	                 "ORDER BY count DESC " +
	                 "LIMIT 1";
	    Statement stmt = con.createStatement();
	    ResultSet rs = stmt.executeQuery(sql);
	    if (rs.next()) {
	        return rs.getString("c_mktsegment");
	    }
	    return null;
	}

	public ResultSet query3() throws SQLException {
	    System.out.println("Executing query #3.");
	    String sql = "SELECT o_orderpriority, COUNT(*) as lineitem_count " +
	                 "FROM dev.lineitem " +
	                 "JOIN dev.orders ON l_orderkey = o_orderkey " +
	                 "WHERE o_orderdate >= '1997-04-01' " +
	                 "AND o_orderdate < '2003-04-01' " +
	                 "GROUP BY o_orderpriority " +
	                 "ORDER BY o_orderpriority ASC";
	    Statement stmt = con.createStatement();
	    ResultSet rs = stmt.executeQuery(sql);
	    System.out.println("Query #3 result: "+resultSetToString(rs, 100));
	    return rs;
	}

	public static String resultSetToString(ResultSet rst, int maxrows) throws SQLException {
		StringBuffer buf = new StringBuffer(5000);
		int rowCount = 0;
		ResultSetMetaData meta = rst.getMetaData();
		buf.append("Total columns: " + meta.getColumnCount());
		buf.append('\n');
		if (meta.getColumnCount() > 0)
			buf.append(meta.getColumnName(1));
		for (int j = 2; j <= meta.getColumnCount(); j++)
			buf.append(", " + meta.getColumnName(j));
		buf.append('\n');
		while (rst.next()) {
			if (rowCount < maxrows) {
				for (int j = 0; j < meta.getColumnCount(); j++) {
					Object obj = rst.getObject(j + 1);
					buf.append(obj);
					if (j != meta.getColumnCount() - 1)
						buf.append(", ");
				}
				buf.append('\n');
			}
			rowCount++;
		}
		buf.append("Total results: " + rowCount);
		return buf.toString();
	}

	public static String resultSetMetaDataToString(ResultSetMetaData meta) throws SQLException {
		StringBuffer buf = new StringBuffer(5000);
		buf.append(meta.getColumnName(1) + " (" + meta.getColumnLabel(1) + ", " + meta.getColumnType(1) + "-"
				+ meta.getColumnTypeName(1) + ", " + meta.getColumnDisplaySize(1) + ", " + meta.getPrecision(1) + ", "
				+ meta.getScale(1) + ")");
		for (int j = 2; j <= meta.getColumnCount(); j++)
			buf.append(", " + meta.getColumnName(j) + " (" + meta.getColumnLabel(j) + ", " + meta.getColumnType(j) + "-"
					+ meta.getColumnTypeName(j) + ", " + meta.getColumnDisplaySize(j) + ", " + meta.getPrecision(j)
					+ ", " + meta.getScale(j) + ")");
		return buf.toString();
	}
}