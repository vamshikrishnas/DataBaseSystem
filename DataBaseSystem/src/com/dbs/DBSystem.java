package com.dbs;

import java.awt.List;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.InputStreamReader;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Scanner;
import java.util.Vector;

import javax.swing.text.TabExpander;

public class DBSystem {
	
	// Variable Declarations
	
	private static int pageSize;
	private static int numPages;
	private static int tableCount;
	
	public String pathTables;
	public String configFile;
	
	
	private static ArrayList<String> tableNames = new ArrayList<String>();
	
	private static HashMap<String, Long> fileLengthInfo = new HashMap<String,Long>();
	
	// <table_name, vector of pages >
	private static HashMap<String,Vector<TablePageOffset>> dbInfo = new HashMap<String,Vector<TablePageOffset>>();
	
	// Data Structures for Handling LRU
	public static HashMap<String, HashMap<Integer,TablePageOffset> > localPageTable;
	public static LinkedHashMap<Integer,HashMap<Integer,String>> page;
	public static HashMap<Integer,String> globalPageTable;
	//PageIDtoFlush and the RecordID's
	
	public static HashMap<Integer,Vector<Integer>> flushPageInfo;
	public static Vector<Integer> pagesToFlush = new Vector<Integer>();
	
	
	/*
	 * 
	 * Initializes LRU 
	 */
	
	public void initLRU()
	{
		globalPageTable=new HashMap<Integer,String>(numPages);
        page=new LinkedHashMap<Integer, HashMap<Integer, String>>(numPages,0.75f,true);
        flushPageInfo=new HashMap<Integer,Vector<Integer>>();
       
        int i;
        for(i=0;i<numPages;i++)
        {
            globalPageTable.put(i,null);
            page.put(i,null);
            Vector<Integer> vec=new Vector<Integer>();
            flushPageInfo.put(i,vec);
        }
        

        localPageTable=new HashMap<String, HashMap<Integer, TablePageOffset>>();
        try
        {
            for(i=0;i<tableNames.size();i++)
            {
                System.out.println(tableNames.get(i));
            	localPageTable.put(tableNames.get(i),null);
            }
        }
        catch(Exception e)
        {

            e.printStackTrace();
        }
     }
	
	
	
	public boolean checkTable(String tablename)
	{
		
		if(tableNames.contains(tablename))
			return true;
		else
			return false;
	}
	
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
			configFile=configFilePath.toString();
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
				
				// just to get the length of file
				RandomAccessFile rf= new RandomAccessFile(new File(table_Fname),"r");
				fileLengthInfo.put(table_name,rf.length());
				rf.close();
				
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
	
	

	public String getRecord(String tableName, int recordId)
	{
		
		  String tableNameFile=pathTables.concat(tableName);
          tableNameFile=tableNameFile.concat(".csv");
          int i,pageIndex;
          String line="",replaceTableName="";
          int startOffset=0,endOffset=0,availablePage,startLine=0,endLine=0;
          
          // search if that record ID is already available in page
          String found=searchRecordID(tableName,recordId);
          if(found!=null)
          {
             System.out.println("HIT");
             return found;
          }
          
          /*check if any free page available in global page table*/
          availablePage=freePageAvailable();
          // if free page not available then go for page replacement*/
          if(availablePage==-1)
          {
        	  availablePage=replacePageAlgo();
        	  /* get table name from which the old entry is to be invalidated */
              replaceTableName=globalPageTable.get(availablePage);
              /* remove old page entry*/
              localPageTable.get(replaceTableName).remove(availablePage);
             //System.out.println("replaced table name="+replaceTableName+" replaced page="+availablePage);
           }
          
          System.out.println("MISS "+availablePage);
          Vector <TablePageOffset> table=dbInfo.get(tableName);
          /* get start record ,end record and offset from dbInfo for reading the only that page from file*/
          for(i=0;i<table.size();i++)
          {
            if(table.get(i).startRecord <= recordId && table.get(i).endRecord>=recordId)
            {
                startOffset=table.get(i).offSet;
                startLine=table.get(i).startRecord;
                endLine=table.get(i).endRecord;

                if(i<table.size()-1)
                endOffset=table.get(i+1).offSet;

                break;
            }

         }
         pageIndex=i;
         try
         {
        	 HashMap<Integer,String> buffer=new HashMap<Integer, String>();

        	 /* open table file to read page */
        	 RandomAccessFile randomAccessFile=new RandomAccessFile(tableNameFile,"r");

        	 /* goto first record of that page using offset*/
        	 randomAccessFile.seek((long)startOffset);


        	 /*read next n records*/
        	 for(i=0;i< (endLine-startLine+1);i++)
        	 {
        		 line=randomAccessFile.readLine();
        		 if(line==null)
        			 break;
        		 if(startLine+i==recordId)
        			 found=line;
        		 buffer.put(startLine+i,line);

        	 }
        	 page.put(availablePage,buffer);

        	 /* Add newly aquired page number to localpagetable of that table*/

        	 HashMap<Integer,TablePageOffset> vector=localPageTable.get(tableName);
        	 if(vector==null)
        		 vector=new HashMap<Integer, TablePageOffset>();


        	 /* for available pag number store its starting recordID endRecordID and offset*/
        	 vector.put(availablePage,table.get(pageIndex));
        	 localPageTable.put(tableName,vector);


        	 /*put newly loaded page and its related table name in global page table*/
        	 globalPageTable.put(availablePage,tableName);

            randomAccessFile.close();

         }
         catch (Exception e)
         {
        	 e.printStackTrace();
         //System.out.println("Error opening table file"+e);
         }
         return found;
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
	
		String table_Fname=pathTables+tableName+".csv";
		Vector <TablePageOffset> currentTable=dbInfo.get(tableName);
		
		int lastPageNo=currentTable.size()-1;
        HashMap <Integer,String> thisPage;
        
        long freeSpaceInLastPage;
        long fileLength=0;
        String replaceTableName="";
        RandomAccessFile randomAccessFile;

        TablePageOffset lastPageOffset=currentTable.get(lastPageNo);
       
        /*
        try
        {
            randomAccessFile=new RandomAccessFile(table_Fname,"r");
            fileLength=randomAccessFile.length();
            randomAccessFile.close();
        }
        catch(Exception e)
        {
        	System.out.println("Error while opening file in insertRecord "+e);
        	e.printStackTrace();
        }
        
        /*
         * 
         * Need to Handle the Case when the num_writes to the Records overflows the currentPage's FreeSize;
         *
         * 
         */
        
        
        
        freeSpaceInLastPage=pageSize-(fileLengthInfo.get(tableName)-lastPageOffset.offSet);
        
        if(freeSpaceInLastPage>=(record.length()+1) || freeSpaceInLastPage==(record.length()) )
        {
        	 int k;
             boolean pageFound=false;
            // System.out.println("For Record"+record);
             
             if(localPageTable.get(tableName)==null)
             {
            	 int availablePage=freePageAvailable();
            	 /* no free frame available to fetch required page into memory*/
                 if(availablePage==-1)
                 {
                	 	availablePage=replacePageAlgo();
                	 	/* get table name from which the old entry is to be invalidated */
                	 	replaceTableName=globalPageTable.get(availablePage);
                	 	localPageTable.get(replaceTableName).remove(availablePage);
                 }
                 
                 HashMap<Integer,String> buffer=new HashMap<Integer, String>();
            	 try
            	 {
            		 randomAccessFile=new RandomAccessFile(table_Fname,"r");
            			
                     /* goto first record of that page using offset*/
                    randomAccessFile.seek(lastPageOffset.offSet);

                    String line="";
                    int i;

                     /*read next n records*/
                    for(i=0;;i++)
                    {
                        line=randomAccessFile.readLine();

                        if(line==null)
                            break;

                        buffer.put(lastPageOffset.startRecord+i,line);
                    }
                    buffer.put(lastPageOffset.startRecord+i,record);
                    flushPageInfo.get(availablePage).add(lastPageOffset.startRecord+i);
                    page.put(availablePage, buffer);
                    if(!pagesToFlush.contains(availablePage))
                    	pagesToFlush.add(availablePage);
                    
                    randomAccessFile.close();
                    lastPageOffset.endRecord++;
                    
                    HashMap<Integer,TablePageOffset> vector=localPageTable.get(tableName);
               	 	if(vector==null)
               	 		vector=new HashMap<Integer, TablePageOffset>();

                    /* make entry in global page table*/
                    globalPageTable.put(availablePage,tableName);
                    vector.put(availablePage,lastPageOffset);
                    localPageTable.put(tableName,vector);
                    fileLengthInfo.put(tableName,fileLength+record.length()+1 );
            	 }catch(Exception e)
            	 {
            		 e.printStackTrace();
            	 }
             }
             else
             {
             
	             Object [] pageKey=localPageTable.get(tableName).keySet().toArray();
	
	
	             /* check whether that page is available in memory */
	             for(k=0;k<localPageTable.get(tableName).size();k++)
	             {
	                 if(localPageTable.get(tableName).get(pageKey[k]).offSet==lastPageOffset.offSet)
	                 {
	                     pageFound=true;
	                     lastPageNo=Integer.parseInt(pageKey[k].toString());
	                     break;
	                 }
	             }
	             if(pageFound)
	             {
	            	 thisPage=page.get(lastPageNo);
	                 thisPage.put(++lastPageOffset.endRecord,record);
	                 flushPageInfo.get(lastPageNo).add(lastPageOffset.endRecord);
	                 if(!pagesToFlush.contains(lastPageNo))
	                    	pagesToFlush.add(lastPageNo);
	                 page.put(lastPageNo,thisPage);
	                 localPageTable.get(tableName).put(lastPageNo,lastPageOffset);
	             }
	             else
	             {
	            	 int availablePage=freePageAvailable();
	            	 /* no free frame available to fetch required page into memory*/
	                 if(availablePage==-1)
	                 {
	                	 	availablePage=replacePageAlgo();
	                	 	/* get table name from which the old entry is to be invalidated */
	                	 	replaceTableName=globalPageTable.get(availablePage);
	                	 	localPageTable.get(replaceTableName).remove(availablePage);
	                	 	
	                	 	
	                	 	
	                 }
	                 HashMap<Integer,String> buffer=new HashMap<Integer, String>();
	                 try
	                 {
	                    /* open table file to read page */
	                    randomAccessFile=new RandomAccessFile(table_Fname,"r");
	
	                     /* goto first record of that page using offset*/
	                    randomAccessFile.seek(lastPageOffset.offSet);
	
	                    String line="";
	                    int i;
	
	                     /*read next n records*/
	                    for(i=0;;i++)
	                    {
	                        line=randomAccessFile.readLine();
	
	                        if(line==null)
	                            break;
	
	                        buffer.put(lastPageOffset.startRecord+i,line);
	                    }
	                    buffer.put(lastPageOffset.startRecord+i,record);
	                    flushPageInfo.get(availablePage).add(lastPageOffset.startRecord+i);
	                    page.put(availablePage, buffer);
	                    if(!pagesToFlush.contains(availablePage))
	                    	pagesToFlush.add(availablePage);
	                    randomAccessFile.close();
	                    lastPageOffset.endRecord++;
	
	                    /* make entry in global page table*/
	                    globalPageTable.put(availablePage,tableName);
	                    localPageTable.get(tableName).put(availablePage,lastPageOffset);
	                    fileLengthInfo.put(tableName,fileLength+record.length()+1 );
	                }
	
	                 catch (Exception e)
	                 {
	                	 e.printStackTrace();
	                  //   System.out.println("Error="+e);
	                 }
	             }
	             
	             
	             
             }     
        }else
        {
        	int tempOffset,availablePage;
            int tempStart,tempEnd;
            try
            {
                  randomAccessFile=new RandomAccessFile(table_Fname,"r");
                  fileLength=randomAccessFile.length();
                  randomAccessFile.close();

            }
            catch(Exception e)
            {
              	e.printStackTrace();
                 // System.out.println("Error while opening file in insertRecord "+e);
              }

              tempOffset=(int)fileLength+1;
              tempStart=lastPageOffset.endRecord+1;
              tempEnd=tempStart;

              TablePageOffset tempObj=new TablePageOffset();
              tempObj.offSet=tempOffset;
              tempObj.startRecord=tempStart;
              tempObj.endRecord=tempEnd;

              dbInfo.get(tableName).add(tempObj);

              availablePage=freePageAvailable();

              if(availablePage==-1)
              {
                 // System.out.println("free page not found. going for replacement");

                  availablePage=replacePageAlgo();

                  /* get table name from which the old entry is to be invalidated */
                  replaceTableName=globalPageTable.get(availablePage);


                  /* remove old page entry*/

                  localPageTable.get(replaceTableName).remove(availablePage);
              //    System.out.println("replaced table name="+replaceTableName+" replaced page="+availablePage);


              }


              HashMap<Integer,String> buffer=new HashMap<Integer, String>();

              buffer.put(tempEnd,record);

              page.put(availablePage, buffer);

                  /* make entry in global page table and local page table*/
              globalPageTable.put(availablePage,tableName);
              localPageTable.get(tableName).put(availablePage,tempObj);

              try
              {
                //  record=record.replaceAll(" ",",");
            	  FileWriter fw=new FileWriter(new File(table_Fname),true);
            	  fw.write(record+"\n");
            	  fw.close();
              }
              catch (Exception e)
              {
                  e.printStackTrace();
              }
        }
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
		
		// get the tableName corresponding to the page and the records from the LRU.
		FileWriter fw;
		try
		{
			for(int i=0;i<pagesToFlush.size();i++)
			{
				int page_num=pagesToFlush.get(i);
				Vector<Integer> vc=flushPageInfo.get(page_num);
				HashMap<Integer,String> records=page.get(page_num);
				String tableName=globalPageTable.get(page_num);
				String filename=pathTables+tableName+".csv";
				fw=new FileWriter(new File(filename),true);
				for(int l=0;l<vc.size();l++)
				{
					 if(records.containsKey(vc.get(l)))
					 {
						 String rec=records.get(vc.get(l));
						 fw.write(rec+"\n");
						 
				     }
				}
				fw.close();
				dbInfo.get(tableName).get(dbInfo.get(tableName).size()-1).endRecord=localPageTable.get(tableName).get(page_num).endRecord;
				flushPageInfo.get(page_num).clear();
			}
			
		}catch(Exception e)
		{
			e.printStackTrace();
		}
		pagesToFlush.clear();
	}
	
	
	/*
	 * This is an helper method, which checks for any available free pages in the LRU.If so returns the page num.
	 * else returns -1;
	 * 
	 */
	private int freePageAvailable() {
		// TODO Auto-generated method stub
		
		for(int i=0;i<numPages;i++)
        {
            if(globalPageTable.get(i)==null)
                return i;
        }
        return -1;
	}
	
	/*
	 * This method is called to replace the page which is least recently used from the LRU and if the page is modified it wil 
	 * write the contents to the file else just replaces the file.
	 * 
	 */
	
	private int replacePageAlgo()
    {

        FileWriter fw;
		Iterator<Map.Entry<Integer,HashMap<Integer,String>>> it= page.entrySet().iterator();
        Map.Entry<Integer,HashMap<Integer,String>> last=null;
        last=it.next();
        int page_num=last.getKey();
        if(pagesToFlush.contains(page_num))
        {
        	Vector<Integer> vc=flushPageInfo.get(page_num);
        	HashMap<Integer,String> records=page.get(page_num);
        	String tableName=globalPageTable.get(page_num);
        	String filename=pathTables+tableName+".csv";
        	try
        	{
	        	fw=new FileWriter(new File(filename),true);
	        	for(int l=0;l<vc.size();l++)
	        	{
	        		if(records.containsKey(vc.get(l)))
	        		{	
	        			String rec=records.get(vc.get(l));
	        			fw.write(rec+"\n");
	        		}
	        	}
	        	fw.close();
	        	dbInfo.get(tableName).get(dbInfo.get(tableName).size()-1).endRecord=localPageTable.get(tableName).get(page_num).endRecord;
	        	flushPageInfo.get(page_num).clear();
        	}catch(Exception e)
        	{
        			e.printStackTrace();
        	}
        	//System.out.println(page_num);
        	pagesToFlush.removeElement(page_num);
        }
        return last.getKey();

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
	
	
	/*
	 * this is an helper method used by get record method, this methd searches for the page in LRU, 
	 * if foung returns the record string else return s null.
	 * 
	 */
	
	public String searchRecordID(String tableName,int recordID)
    {
        String result=null;
        int i,pageIndex;

        /* get local page table for current table */
        HashMap<Integer,TablePageOffset> localPages=localPageTable.get(tableName);

        if(localPages==null)
            return null;

        /* store all its page numbers already present*/
        Object [] pageNos=localPages.keySet().toArray();

        /* for each pagenumber check if required recordID lies in that page*/
        for(i=0;i<pageNos.length;i++)
        {
            if(localPages.get(pageNos[i]).startRecord <= recordID && recordID <= localPages.get(pageNos[i]).endRecord)
            {

                /* if required recordID present in current page then put that page again to increase its access order in linkedHashMap*/
                page.put(Integer.parseInt(pageNos[i].toString()),page.get(Integer.parseInt(pageNos[i].toString())));

                      break;
            }
        }
          if(i==pageNos.length)
          {
                return null;
          }

        HashMap <Integer,String> recordLine=null;
        recordLine=page.get(pageNos[i]);

        result=recordLine.get(recordID);

        return result;
    }
	
	
	
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
				System.out.println("START REC"+t.startRecord + " "+"END REC"+ t.endRecord + " " + t.offSet);
			}
		}
		
	}

	
	/*
	 * 
	this method adds the records the pagetable corresponding to the table name.
	 * 
	 */
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
	
	
	
	public void printtableFile(String str) 
	{
		String tname= pathTables.concat(str+".csv");
		
		try
		{
			FileInputStream fis = new FileInputStream(new File(tname));
			BufferedReader br = new BufferedReader(new InputStreamReader(fis));
		
			String line = null;
			int len=0;
			int offset=100;
			while ((line = br.readLine()) != null) {
				
				System.out.print(len+".");
				System.out.println(line);
				System.out.println(line.length()+1);
				len++;
			}
		
			br.close();
			fis.close();
		}catch(Exception e)
		{
			e.printStackTrace();
		}
	}
	public static void main(String[] args)
	{
		DBSystem dbs=new DBSystem();
		dbs.readConfig("/home/vamshi_s/Downloads/Downloads/testCase/");
		dbs.populateDBInfo();
		
		System.out.println("\n \n ");
		
		
		dbs.printtableFile("countries");
		
		
		System.out.println("\n \n ");
		// Query Student Table
		
		System.out.println("\nSearching for Records\n");
		dbs.initLRU();
	
		
		
		dbs.getRecord("countries",0); // miss 0 <page1>
		dbs.getRecord("countries",1); // hit <page1>
		dbs.getRecord("countries",2); // hit <page1>
		dbs.insertRecord("countries", "record"); // miss 1
		dbs.getRecord("countries",2); //hit <page1>
		dbs.getRecord("countries",2); //hit <page1>
		dbs.getRecord("countries",3); // miss 2 <page2>
		dbs.getRecord("countries",41); // miss 3 <page12>
		
		dbs.getRecord("countries",9); //call pagereplace,replace page 1 with  miss1<page3>
		dbs.getRecord("countries",39); // hit <page12>
		
		dbs.getRecord("countries",28); //miss 0 <page9>
		dbs.getRecord("countries",1);  //miss 2 <page1>
		
		dbs.getRecord("countries",30); // hit 0 <page9>
		
		dbs.getRecord("countries",38); // hit <page12>
		dbs.getRecord("countries",39); //hit <page12>
		dbs.getRecord("countries",31); //miss 1 <page10>
		dbs.insertRecord("countries", "record"); //replace 0, page9
		dbs.getRecord("countries",42); 
		dbs.getRecord("countries",28);
		
		/*
		 * 
		 * MISS 0
			HIT
			HIT
			HIT
			HIT
			MISS 2
			MISS 3
			MISS 1
			HIT
			MISS 0
			MISS 2
			MISS 1
			MISS 3
			MISS 0
			MISS 2
			MISS 3
			MISS 0
		 * 
		 */
		
		dbs.flushPages();
	}
}
