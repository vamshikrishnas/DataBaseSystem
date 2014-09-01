package com.dbs;

import java.awt.List;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Scanner;
import java.util.Vector;

public class DBSystem {
	
	// Variable Declarations
	
	private static int pageSize;
	private static int numPages;
	private static int tableCount;
	
	public String pathTables;
	
	private static ArrayList<String> tableNames = new ArrayList<String>();
	
	// <table_name, vector of pages >
	private static HashMap<String,Vector<TablePageOffset>> dbInfo = new HashMap<String,Vector<TablePageOffset>>();
	
	
	/* 
	 * 
	 * 
	 * Description :
	 * You need to read the configuration file and extract the page size and number 
	 * of pages (these two parameter together define the maximum main memory you can use). 
	 * Values are in number of bytes.You should read the names of the tables from the configuration file.
	 * You can assume that the table data exists in a file named
	 * <table_name>.csv at the path pointed by the config parameter PATH_FOR_DATA.
	 * You will need other metadata information given in config file for future deliverables. 
	 *
	 */
	
	public void readConfig(String configFilePath)
	{
		try
		{
			
			configFilePath=configFilePath+"config.txt";
			Scanner configReader=new Scanner(new File(configFilePath));
			configReader.useDelimiter("\n");
			String params ;
			
			// get the Page Size from the Configuration File
			params = configReader.next();
			params = params.substring(params.indexOf(" ")+1, params.length());
			pageSize=Integer.parseInt(params);
			
			// get the numpages from the configuration file
			params = configReader.next();
			params = params.substring(params.indexOf(" ")+1, params.length());
			numPages=Integer.parseInt(params);
			
			//get the path to the csv files from the configuration file
			params = configReader.next();
			pathTables = params.substring(params.indexOf(" ")+1, params.length()).toString();
			
			/* read the information about the tables;
			 * Format of the table
			 *  BEGIN
					<table _ name >
					employee_id,int
					employee_name,string
				END
			 *
			 */
			
			boolean begin=false;
			String tempTablename;
			
			// initialize table count to maintain the num ber of the tbales in DB
			tableCount=0;
			
			
			while(configReader.hasNext())
			{
				params=configReader.next();
				if(!begin && params.contentEquals("BEGIN"))
				{
					begin=true;
					if(configReader.hasNext())
					{
						tempTablename=configReader.next();
						tableNames.add(tempTablename);
						tableCount++;
						
					}else
					{
						System.out.println("Invalid Contents in the ConfigFile");
						System.out.println("DBSystem Exiting");
						System.exit(0);
					}
					
				}else if (begin && params.contentEquals("END"))
				{
					begin=false;
					
				}else 
				{
					// Read the Attributes of the table.
					
					
					
				}
				
			} // end of reading config file
			
			configReader.close();
			
			
			
		}catch (FileNotFoundException e)
		{
			System.out.println("File Not Found // Config File\n");
			e.printStackTrace();
		}catch (Exception e)
		{
			e.printStackTrace();
		}
		
		// Call to testing Function
		tester();
	
	}
	
	/* Description:
	 * 
	 * The data present in each table needs to be represented in pages.
	 * Read the file corresponding to each table line by line (for now assume 1 line = 1 record).
	 * Maintain a mapping from PageNumber to (StartingRecordId, EndingRecordId) in memory.
	 * You can assume unspanned file organisation and record length will
	 * not be greater than page size. 
	*/
	
	public void populateDBInfo()
	{
		
		try
		{
			Iterator<String> it=tableNames.iterator();
			String table_name;
			String table_Fname;
			String record;
			
			int currentFree=0;
			int currentOffset,currentRecord,recordLength=0;
			int nextRecordOffset=0,nextRecordSize=0;
			int newLine = 0;
			
			boolean newPage=true,firstPage = true,pageAdded = false,lastRecord=false;
			
			TablePageOffset pageData = new TablePageOffset();
			
			// Reading all the tables Data into pages
			
			
			while(it.hasNext())
			{
				table_name=it.next();
				table_Fname=pathTables+table_name+".csv";
				Scanner tableReader = new Scanner(new File(table_Fname));
				tableReader.useDelimiter("\n");
				
				currentOffset = 0;
				currentRecord = 0;
				
				firstPage = true;
				newPage=true;
				
				nextRecordOffset=0;
				nextRecordSize=0;
				
				lastRecord = false ;
				newLine = 0;
				
				// Read the rows of the table
				
				while(tableReader.hasNext())
				{
					if(newPage)
					{
						pageData = new TablePageOffset();
						pageData.startRecord=currentRecord;
						pageData.offSet=currentOffset;
						
						currentOffset+=nextRecordOffset;
						currentFree = pageSize-nextRecordSize;
						
						newPage= false;
						newLine=1;
						pageAdded=false;
						record=null;
						
					}else
					{
						record=tableReader.next();
						recordLength=record.length();
						if(!tableReader.hasNext() && record!=null)
							lastRecord=true;
						currentOffset+=recordLength+1;
						if((currentFree-recordLength-1)<0 && (currentFree - recordLength)==0)
						{
							newLine = 0;						
						}
						currentFree-= (recordLength+newLine);
						currentRecord++;
						if(currentFree<0)
						{
							if(firstPage)
							{	
									currentRecord--;
									firstPage = false;
							}
							pageData.endRecord = currentRecord -1;
							currentOffset -= (recordLength+1); 
							
							nextRecordOffset = recordLength+newLine; 
							nextRecordSize = recordLength+0;	
							
							addRecordtoDB(table_name,pageData);
							
							newPage = true;
							pageAdded = true;
						}
					}
				}
				
				if(lastRecord && pageAdded)
				{
					pageData = new TablePageOffset(); 
					pageData.startRecord = currentRecord;
					pageData.offSet = currentOffset ;
					pageData.endRecord = currentRecord;
					addRecordtoDB(table_name,pageData);
				}else if(lastRecord && firstPage)
				{
					pageData.endRecord=currentRecord-1;
					addRecordtoDB(table_name, pageData);
				}
				else if(lastRecord && !pageAdded) 
				{
					pageData.endRecord = currentRecord;
					addRecordtoDB(table_name, pageData);
				}
				
				tableReader.close();
			}
			
			
		}catch(Exception e)
		{
			e.printStackTrace();
		}
		
		printDBinfo();
		
	}
	
	
	/* Description:
	 * 
	 * Get the corresponding record of the specified table.DO NOT perform I/O every time. 
	 * Each time a request is received, if the page containing the record is already in memory, 
	 * return that record else bring corresponding page in memory. You are supposed 
	 * to implement LRU page replacement algorithm for the same. Print HIT if the page is in memory, 
	 * else print MISS <pageNumber> where <pageNumber> is the page number of memory page which is to be replaced. (You can assume page
	 * numbers starting from 0. So, you have total 0 to <NUM_PAGES 1>pages.) 
	 * 
	 */
	
	private void printDBinfo() {
		// TODO Auto-generated method stub
		
		Iterator<String> it = dbInfo.keySet().iterator();
		Iterator<Vector<TablePageOffset>> pages = dbInfo.values().iterator();
		TablePageOffset t ;
		while(it.hasNext())
		{
			System.out.println(it.next());
			Iterator<TablePageOffset> pg = pages.next().iterator();
			while(pg.hasNext())
			{
				t = pg.next();
				System.out.println(t.startRecord + " " + t.endRecord + " " + t.offSet);
			}
		}
		
	}

	private void addRecordtoDB(String table_name, TablePageOffset pageData) {
		// TODO Auto-generated method stub
		
			if(dbInfo.containsKey(table_name))
			{
				dbInfo.get(table_name).add(pageData);
			}else
			{
				Vector<TablePageOffset> pageOffsets=new Vector<TablePageOffset>();
				pageOffsets.add(pageData);
				dbInfo.put(table_name, pageOffsets);
			}
		
		
	}

	
	
	
	
	public String getRecord(String tableName, int recordId)
	{
		String record = null;
		String tableFname=pathTables+tableName+".csv".toString();
		try
		{
			
			
			Vector<TablePageOffset> t_pages=dbInfo.get(tableName);
			if(t_pages==null)
				 	return null;
			 
			 int sOfset = 0,sLine = 0,eLine = 0,eOfset;
			// System.out.println(t_pages.size());
			 for(int i=0;i<t_pages.size();i++)
	          {
	               if(t_pages.get(i).startRecord <= recordId && t_pages.get(i).endRecord>=recordId)
	               {
	            	   sOfset=t_pages.get(i).offSet;
	                   sLine=t_pages.get(i).startRecord;
	                   eLine=t_pages.get(i).endRecord;

	                   if(i<t_pages.size()-1)
	                	   eOfset=t_pages.get(i+1).offSet;
	                   break;
	               }
	          }
			 
			 RandomAccessFile rf=new RandomAccessFile(tableFname, "r");
			 rf.seek((long)sOfset);
			 
			 for(int i=0;i<(eLine-sLine+1);i++)
	         {
	                record=rf.readLine();

	                if(record==null)
	                    break;

	                if(sLine+i==recordId)
	                    return record;
	         }
			 
	}catch(Exception e)
		{
			e.printStackTrace();
		}
		
		return record;
		
		
		
	}
	
	
	/* Description:
	 * 
	 *  Get the last page for the corresponding Table in main memory, if not already present.
	 *  If the page has enough free space, 
	 *  then append the record to the page else, 
	 *  get new free page and add record to the new page.Do not flush modified page immediately.
	 *  
	 */
	
	public void insertRecord(String tableName, String record)
	{
		
	
	}
	
	/* Description:
	 * 
	 * Since primary and secondary memory are independent, no need to
	 * flush modified pages immediately, instead this function will be called
	 * to write modified pages to disk.
	 * Write modified pages in memory to disk.
	 * 
	 */
	
	public void flushPages()
	{
	
	}
	
	
	/*
	 * 
	 * Tester Funtion 
	 */
	
	public void tester()
	{
		System.out.println("Page Size: "+pageSize);
		System.out.println("Num Pages: "+numPages);
		System.out.println("Num Tables: "+tableCount);
		System.out.println("File Data Path: "+pathTables);
		System.out.println("Printing Table Names");
		
		Iterator it = tableNames.iterator();
		while(it.hasNext())
		{
			System.out.println("Table Name: "+it.next().toString());
		}
	}
	
	
	public static void main(String[] args)
	{
		DBSystem dbs=new DBSystem();
		dbs.readConfig("/home/vamshi_s/Downloads/Downloads/test/");
		dbs.populateDBInfo();
		
		/*
		// Query Student Table
		
		System.out.println("\nSearching for Records\n");
		
		System.out.println(dbs.getRecord("student", 19));
		System.out.println(dbs.getRecord("student", 0));
		System.out.println(dbs.getRecord("student", 26));
		
		//Query Customers Table
		
		System.out.println(dbs.getRecord("Customers", 3));
		
		//Query Orders Table
		
		System.out.println(dbs.getRecord("Orders", 2));
		System.out.println(dbs.getRecord("Orders", 19));
		System.out.println(dbs.getRecord("Orders", 14));
		 */	
		
	}
	
	
	

}
