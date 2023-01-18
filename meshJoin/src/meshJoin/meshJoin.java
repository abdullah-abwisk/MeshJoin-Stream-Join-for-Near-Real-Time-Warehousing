package meshJoin;

import java.io.File;
import java.nio.channels.SelectableChannel;
import java.sql.*;
import java.sql.Date;
import java.util.*;
import com.mysql.cj.xdevapi.Schema.CreateCollectionOptions;

import java.util.concurrent.ArrayBlockingQueue;

public class meshJoin {
	public static void main(String[] args) {
		long start = System.currentTimeMillis();
		Connection connTrans = null;
		Connection connMaster = null;
		Connection connWarehouse = null;
		Statement StmtTrans = null;
		Statement StmtProds = null;
		Statement StmtCusts = null;
		Statement StmtWarehouse = null;
		
		String url;
		String user = "root";
		String password = "22385524";
		String database = "db";
		String warehouse = "dw";
		
		char changecreds = 'n';
		Scanner input = new Scanner(System.in);
		System.out.println("Do you want to change default credentials for Database? (y/n).");
		changecreds = input.next().charAt(0);
		
		if(changecreds == 'y') {
			System.out.print("Enter Username(root): ");
			user = input.next();
			System.out.print("Enter Password: ");
			password = input.next();
			System.out.print("Enter Database Name: ");
			database = input.next();
		}
		
		try {
			Class.forName("com.mysql.cj.jdbc.Driver");
			url = "jdbc:mysql://127.0.0.1:3306/" + database;
			connTrans = DriverManager.getConnection(url, user, password);
			connMaster = DriverManager.getConnection(url, user, password);
			url = "jdbc:mysql://127.0.0.1:3306/" + warehouse;
			connWarehouse = DriverManager.getConnection(url, user, password);
			System.out.println("Connections Successful.");
			StmtTrans = connTrans.createStatement();
			StmtProds = connMaster.createStatement();
			StmtCusts = connMaster.createStatement();
			StmtWarehouse = connWarehouse.createStatement();
			ResultSet transactions, products, customers;
			ResultSet noOfTrans = StmtTrans.executeQuery("SELECT COUNT(*) FROM db.TRANSACTIONS");
			noOfTrans.next();
			int noOfTransactions = noOfTrans.getInt(1);
			int current = 0;
			int currentProds = 0;
			int currentCusts = 0;
			String trans, prods, custs;
			String prodidmaster, prodnamemaster, suppidmaster, suppnamemaster, custidmaster, custnamemaster;
			Double price;
			Boolean comp = false;
			Queue<ArrayList<ArrayList<String>>> transactionQueue = new LinkedList<ArrayList<ArrayList<String>>>();
	        HashMap<ArrayList<String>, ArrayList<String>> hashmap = new HashMap<ArrayList<String>, ArrayList<String>>();		
	        System.out.println("Starting stream....");
	        while(current < noOfTransactions || !(transactionQueue.isEmpty())) {
				ArrayList<ArrayList<String>> part = new ArrayList<ArrayList<String>>();
				ArrayList<ArrayList<String>> curpart = new ArrayList<ArrayList<String>>();
				trans = "SELECT * FROM TRANSACTIONS LIMIT " + current + ", 100";
	            prods = "SELECT * FROM PRODUCTS LIMIT " + currentProds + ", 20";
	            custs = "SELECT * FROM CUSTOMERS LIMIT " + currentCusts + ", 10";
	            transactions = StmtTrans.executeQuery(trans);
	            products = StmtProds.executeQuery(prods);
	            customers = StmtCusts.executeQuery(custs);	            
	            if(current < noOfTransactions) {
	            	while(transactions.next()) {
		            	ArrayList<String> lst = new ArrayList<String>();
		            	ArrayList<String> values = new ArrayList<String>();
		                ArrayList<String> keys = new ArrayList<String>();
		            	String tid = transactions.getString("TRANSACTION_ID");
		            	String prodid = transactions.getString("PRODUCT_ID");
		            	String custid = transactions.getString("CUSTOMER_ID");
		            	String storeid = transactions.getString("STORE_ID");
		            	String storename = transactions.getString("STORE_NAME");
		            	String timeid = transactions.getString("TIME_ID");
		            	String date = transactions.getString("T_DATE");
		            	String quantity = transactions.getString("QUANTITY");
		                keys.add(tid);
		                keys.add(prodid);
		                keys.add(custid);
		                values.add(tid);
		                values.add(prodid);
		                values.add(custid);
		                values.add(storeid);
		                values.add(storename);
		                values.add(timeid);
		                values.add(date);
		                values.add(quantity);
		                values.add("");
		                values.add("");
		                values.add("");
		                values.add("");
		                values.add("");
		                hashmap.put(keys, values);
		                lst.add(tid);
		                lst.add(prodid);
					    lst.add(custid);
		                part.add(lst);
		            }
		            transactionQueue.add(part);
	            }

	            while(products.next()) {
					prodidmaster = products.getString("PRODUCT_ID");
					prodnamemaster = products.getString("PRODUCT_NAME");
					suppidmaster = products.getString("SUPPLIER_ID");
					suppnamemaster = products.getString("SUPPLIER_NAME");
					price = products.getDouble("PRICE");
					for (ArrayList<String> k : hashmap.keySet()) {
						if (k.get(1).equals(prodidmaster)) {
							ArrayList<String> v = hashmap.get(k);
							v.set(8, prodnamemaster);
							v.set(9, suppidmaster);
							v.set(10, suppnamemaster);
							v.set(11, price.toString());
							hashmap.put(k, v);
						}
					}
	            }
	            
				while(customers.next()) {
				custidmaster = customers.getString("CUSTOMER_ID");
					custnamemaster = customers.getString("CUSTOMER_NAME");
					for (ArrayList<String> k : hashmap.keySet()) {
						if (k.get(2).equals(custidmaster)) {
							ArrayList<String> v = hashmap.get(k);
							v.set(12, custnamemaster);
							hashmap.put(k, v);
						}
					}
				}
	             
	            // Printing to test
	            // System.out.println("Number of partitions in queue: " + transactionQueue.size());
	            // System.out.println("Queue: " + transactionQueue);
	            // System.out.println("Number of keys in hashmap: " + hashmap.size());
	            // System.out.println("HashMap: " + hashmap);
	            
	            current += 100;
	            currentProds += 20;
	            currentCusts += 10;
	            
	            if(currentProds == 100) {
	            	comp = true;
	            	currentProds = 0;
	            	currentCusts = 0;
	            }
	            
	            if(comp == true) {
	                curpart = transactionQueue.poll();
	                for (int i = 0; i < 100; i++) {
	                	ArrayList<String> values = hashmap.get(curpart.get(i));
	                	String sql = "INSERT INTO PRODUCTS VALUES (?, ?, ?)";
						PreparedStatement stmt0 = connWarehouse.prepareStatement(sql);
						stmt0.setString(1, values.get(1));
						stmt0.setString(2, values.get(8));
						stmt0.setString(3, values.get(11));
						try {
							stmt0.executeUpdate();
						}
						catch(Exception e) {	}
						sql = "INSERT INTO CUSTOMERS VALUES (?, ?)";
						PreparedStatement stmt1 = connWarehouse.prepareStatement(sql);
						stmt1.setString(1, values.get(2));
						stmt1.setString(2, values.get(12));
						try {
							stmt1.executeUpdate();
						}
						catch(Exception e) {	}
						sql = "INSERT INTO STORES VALUES (?, ?)";
						PreparedStatement stmt2 = connWarehouse.prepareStatement(sql);
						stmt2.setString(1, values.get(3));
						stmt2.setString(2, values.get(4));
						try {
							stmt2.executeUpdate();
						}
						catch(Exception e) {	}
						sql = "INSERT INTO SUPPLIERS VALUES (?, ?)";
						PreparedStatement stmt3 = connWarehouse.prepareStatement(sql);
						stmt3.setString(1, values.get(9));
						stmt3.setString(2, values.get(10));
						try {
							stmt3.executeUpdate();
						}
						catch(Exception e) {	}
						sql = "INSERT INTO TIMES VALUES (?, ?, ?, ?, ?)";
						PreparedStatement stmt4 = connWarehouse.prepareStatement(sql);
						String currDate = values.get(6);
						String year = currDate.substring(0, 4);
                        String month = currDate.substring(5, 7);
                        String day = currDate.substring(8, 10);
                        int yearInt = Integer.parseInt(year);
                        int monthInt = Integer.parseInt(month);
                        int dayInt = Integer.parseInt(day);
                        stmt4.setString(1, values.get(5));
                        stmt4.setString(2, currDate);
                        stmt4.setInt(3, dayInt);
                        stmt4.setInt(4, monthInt);
                        stmt4.setInt(5, yearInt);
						try {
							stmt4.executeUpdate();
						}
						catch(Exception e) {	}
						sql = "INSERT INTO TRANSACTIONS VALUES (?, ?, ?, ?, ?, ?, ?, ?)";
						PreparedStatement stmt5 = connWarehouse.prepareStatement(sql);
						stmt5.setString(1, values.get(0));
						stmt5.setString(2, values.get(1));
						stmt5.setString(3, values.get(2));
						stmt5.setString(4, values.get(5));
						stmt5.setString(5, values.get(9));
						stmt5.setString(6, values.get(3));
						stmt5.setString(7, values.get(7));
						Double sale = Double.parseDouble(values.get(7)) * Double.parseDouble(values.get(11));
						stmt5.setDouble(8, sale);
						try {
							stmt5.executeUpdate();
						}
						catch(Exception e) {	}
	                    hashmap.remove(curpart.get(i));
	                }
	                System.out.println("100 Transaction loaded....");
	            }
			}
	        System.out.println("All transactions loaded into Warehouse.");
	        long end = System.currentTimeMillis();
			long elapsedTime = end - start;
			System.out.println("Time taken: " + (elapsedTime / 1000) + "s");
		}
		catch(Exception e) {
			System.out.println("Connections Failed.");
			e.printStackTrace();
		}
	}
}
