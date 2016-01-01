/* 
=====================================================================
     
    
 		MICHAEL J. SIDERIUS
  		
  		STOCK INVESTMENT STRATEGY
  		NOVEMBER 10 2015
  		
 
       
=====================================================================
*/


import java.util.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.io.FileInputStream;
import java.lang.reflect.Array;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

class data {

	static Connection conn = null;

	public static double cash = 0;
	public static double shares = 0;
	public static int transExec = 0;
	public static int rx = 0;

	public static void main(String[] args) throws Exception {
		// Get connection properties
		String paramsFile = "ConnectionParameters.txt";
		if (args.length >= 1) {
			paramsFile = args[0];
		}
		Properties connectprops = new Properties();
		connectprops.load(new FileInputStream(paramsFile));

		try {
			// Get connection
			Class.forName("com.mysql.jdbc.Driver");
			String dburl = connectprops.getProperty("dburl");
			String username = connectprops.getProperty("user");
			conn = DriverManager.getConnection(dburl, connectprops);
			System.out.printf("Database connection %s %s established.%n", dburl, username);
			Scanner in = new Scanner(System.in);

			boolean bool = true;
			while (bool) {
				System.out.print("\n" + "Enter a ticker symbol [start/end dates]: ");
				String[] data = in.nextLine().trim().split("\\s+");

				//  ONLY TICKER IS ENTERED
				String ticker = "";
				String startDate = "";
				String endDate = "";

				//  NOTHING IS ENTERED , EXCEPTION CHECKING
				if (data[0].equals("")) {
					System.out.println("Database connection closed.");
					System.exit(1);
				}
				// if a ticker and dates are entered
				if (data.length == 3) {
					ticker = data[0];
					startDate = data[1];
					endDate = data[2];
					rx = 1;
				} 
				// if only a ticker is entered
				else if (data.length == 1) {
					ticker = data[0];
				} 
				else {
					System.out
							.println("You have entered the wrong amount of arguments that this program is able to use");
				}
				if (showName(ticker)) {
					driver(ticker, startDate, endDate);
				}

			}

			conn.close();
		} catch (SQLException ex) {
			System.out.printf("SQLException: %s%nSQLState: %s%nVendorError: %s%n", ex.getMessage(), ex.getSQLState(),
					ex.getErrorCode());
		}
	}
	
	
	/*
	 * THis is the main function in which drives most of the program and helper functions are being called within it. 
	 * It detects splits, and deals with trading days, split totals and dates.
	 * 
	 * INPUT: Ticker, startDate and a endDate
	 * OUTPUT: USER DATA printouts
	 */
	static void driver(String ticker, String startDate, String endDate) throws SQLException, ParseException {
		
		// Initiations
		int splitDaysTotal = 0;
		String compareDate = "";
		
		double closingPrice, openingPrice, highPrice, lowPrice;
		double op = 0.0;
		double varOp = 0.0;

		String varDate = "";
		String dateTrail = "";
		String date;
		String twoOne = "";
		String threeOne = "";
		String threeTwo = "";

		// Main SQL query used
		PreparedStatement pstmt = conn
				.prepareStatement("select* from PriceVolume where Ticker = ? order by TransDate ASC");
		pstmt.setString(1, ticker);

		ResultSet rs = pstmt.executeQuery();

		TreeMap<String, double[]> hash = new TreeMap<String, double[]>();
		int tradingDays = 0;
		
		// Iterate over the query data
		while (rs.next()) {
			date = rs.getString("TransDate");
			closingPrice = rs.getDouble("ClosePrice");
			openingPrice = rs.getDouble("OpenPrice");
			highPrice = rs.getDouble("HighPrice");
			lowPrice = rs.getDouble("LowPrice");
			
			// This is for when a ticker is only entered
			if (!(startDate.equals("") && endDate.equals(""))) {
				compareDate = date;
			}
			// Used to keep track of d-1 d and d+1 closed price data for calculations
			varDate = date;
			op = closingPrice;
			
			if (dateHelperFunction(endDate, startDate, compareDate) || (startDate.equals("") && endDate.equals(""))) {
				tradingDays++;
				double[] a = new double[5];
				
				a[0] = closingPrice;
				a[1] = openingPrice;
				a[2] = highPrice;
				a[3] = lowPrice;
				
				// 2:1 Split
				if ((Math.abs((varOp / openingPrice) - 2.0)) < 0.2) {
					twoOne = "2:1 split on " + dateTrail + " " + varOp + " --> " + openingPrice + "\n" + twoOne;
					hash = adjust(hash, 2.0);
					splitDaysTotal++;
				}
				// 3:1 Split
				if ((Math.abs((varOp / openingPrice) - 3.0)) < .3) {
					threeOne = "3:1 split on " + dateTrail + " " + varOp + " --> " + openingPrice + "\n" + threeOne;
					hash = adjust(hash, 3.0);
					splitDaysTotal++;
				}
				// 3:2 Split
				if ((Math.abs((varOp / openingPrice) - 1.5)) < .15) {
					threeTwo += "3:2 split on " + dateTrail + " " + varOp + " --> " + openingPrice + "\n" + threeTwo;
					hash = adjust(hash, 1.5);
					splitDaysTotal++;
				}
				hash.put(date, a);
			}
			// Used to keep track of d-1 d and d+1 closed price data for calculations
			varOp = op;
			dateTrail = varDate;
		}
		// USER printouts
		System.out.print(twoOne);
		System.out.print(threeOne);
		System.out.println(threeTwo);

		if (rx == 1) {
			tradingDays++;
		}
		System.out.println(splitDaysTotal + " splits in " + tradingDays + " trading days");
		System.out.println();
		pstmt.close();
		hash = strategy(hash);
		cash = shares * cash;
		shares = 0;
		rx = 0;
	}
	
	/*
	 *This strategy function is responsible for caluclating the moving averages along with the 
	 *net profit and trasactions. It is done by using a deque. 
	 * 
	 * INPUT: TreeMap in which needs to be reset to update the system
	 * OUTPUT: Treemap which is resetting the main treemap to essentially update the system
	 */

	public static TreeMap<String, double[]> strategy(TreeMap<String, double[]> tree) {

		Deque<String> q = new LinkedList<String>();
		
		double prevClose = 0.0;
		boolean tag = false;
		int totalC = 0;
		double avg = 0.0;
		
		// Loop through all of the keys in the tree
		for (String date : tree.keySet()) {
			totalC++;
			q.addLast(date);
			
			if (q.size() == 51) {
				// The value in which the data is going into
				String d = q.peekLast();
				double averageNumerator = 0.0;
				int count = 0;
				
				// This runs over the queue q-1 times to account for the 51st value in which we use as a day key where  to store the information
				for (String x : q) {
					count++;
					
					if (count == 51) {
						break;
					}
					// Average calculations
					averageNumerator += tree.get(x)[0];
					avg = averageNumerator / 50;
					// storing all moving averages as the 4th index in tree
					tree.get(d)[4] = avg;
				}
				averageNumerator = 0;
				q.remove();
			}
			// This is the buy criteron, it is seperated to produce the neccesary data, d and d+1 d-1
			if (tag == true) {
				double[] a = tree.get(date);
				cash -= 100 * a[1] + 8;
				shares += 100;
				transExec++;

			}
			tag = false;
			// CRITERON
			if (totalC > 51) {
				double close = tree.get(date)[0];
				double open = tree.get(date)[1];
				// This is the buy criteron, see above
				if (close < avg && close / open < 0.97000001) {
					tag = true;
				}
				// This is the sell criteron. 
				else if ((shares >= 100 && open > avg && open / prevClose > 1.00999999)) {
					shares -= 100;
					cash += 100 * (close + open) / 2;
					cash -= 8.0;
					transExec++;
				}
			}
			prevClose = tree.get(date)[0];
		}
		System.out.println("Executing investment strategy");
		System.out.println("Transactions executed: " + transExec);
		System.out.println("Net cash: " + Math.round(cash * 100.0) / 100.0);
		transExec = 0;
		
		return tree;
	}

	/*
	 * This adjust function adjusts the treemap by dividing all values within it
	 * by the div variable, which is determined by which split has occurred. It
	 * returns a tree map, in which should be reset to the main tree to update
	 * it.
	 * 
	 * INPUT: TreeMap and a div, which is the split determined value
	 * OUTPUT: A updated tree in which should be reset to the main tree for full update
	 */
	
	public static TreeMap<String, double[]> adjust(TreeMap<String, double[]> map, double div) {
		
		TreeMap<String, double[]> tree = new TreeMap<String, double[]>();

		for (String date : map.keySet()) {
			double[] temp = new double[5];
			double[] ret = new double[5];

			temp = map.get(date);

			ret[0] = temp[0] / div;
			ret[1] = temp[1] / div;
			ret[2] = temp[2] / div;
			ret[3] = temp[3] / div;

			tree.put(date, ret);
		}
		return tree;
	}
	
	/*
	 * This helper function is to determine if a date is in between the
	 * requested start and end date for the stock It outputs true if the compare
	 * date is in between the two dates and false if it does not
	 *
	 * INPUT: endDate, startDate and a compare date 
	 * OUTPUT: a boolean win which returns true if the compare date is in between the end and startDate
	 */
	public static boolean dateHelperFunction(String endDate, String startDate, String compareDate)
			throws ParseException {
		if (compareDate.equals("")) {
			return false;
		}

		SimpleDateFormat sdf = new SimpleDateFormat("yyyy.MM.dd");
		Date d = sdf.parse(compareDate);
		Date min = sdf.parse(startDate);
		Date max = sdf.parse(endDate);

		return d.after(min) && d.before(max);
	}
	
	/*
	 * Retreives the full name within the database It is done with a simple SQL
	 * query. IT also checks for if nothing is entered. 
	 * 
	 * INPUT: Ticker in which the full name will produce
	 * OUTPUT: user data and a boolean for print out purposes in driver
	 */
	public static boolean showName(String ticker) throws SQLException {

		PreparedStatement pstmt = conn.prepareStatement("select Name " + "  from Company " + "  where Ticker = ?");
		pstmt.setString(1, ticker);
		ResultSet rs = pstmt.executeQuery();

		if (rs.next()) {
			System.out.printf(rs.getString("Name"));
			System.out.printf("\n\n");
			pstmt.close();
			return true;
		}
		else {
			System.out.printf(ticker + " not found in database." + "\n\n");
			return false;
		}
	}
}
