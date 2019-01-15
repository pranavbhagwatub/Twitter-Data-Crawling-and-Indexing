import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.MultiFields;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
  
public class InvIndex {
	 
	 String input;
     TreeMap<String, LinkedList<Integer>> hmTermAndPostingList = new TreeMap<String, LinkedList<Integer>>();
     TreeMap<String, LinkedList<Integer>> hmOutputTermAndPostingList = new TreeMap<String, LinkedList<Integer>>();
     HashMap<String, LinkedList<Integer>> unsortedTermAndPostingList = new HashMap<String, LinkedList<Integer>>();
     HashMap<String, LinkedList<Integer>> unsortedOutputTermAndPostingList = new HashMap<String, LinkedList<Integer>>();
     
	  public static void main(String args[]) throws IOException
	  {
		  String input_path_arg= args[2];
		  String output_path_arg=args[1];
		  String index_path_arg=args[0];
		  FileInputStream input= new FileInputStream(new File(input_path_arg));
		  BufferedReader br=new BufferedReader(new InputStreamReader(input,"UTF-8"));
		  Directory index = FSDirectory.open(Paths.get(index_path_arg));
		  IndexReader reader = DirectoryReader.open(index);
		  InvIndex objInvIndex = new InvIndex();
		  
		  //Generating the posting list per field
		  objInvIndex.generatePostingList("text_en",index,reader);
		  objInvIndex.generatePostingList("text_es",index,reader);
		  objInvIndex.generatePostingList("text_fr",index,reader);
		  
		  BufferedWriter writer = new BufferedWriter(new FileWriter(output_path_arg));
		  String termsFromInputFile=new String();
		  String line="";
		  int inputLine=0;
		  while((line=br.readLine())!=null)
		  {
			  inputLine++;
			  objInvIndex.hmOutputTermAndPostingList= new TreeMap<String, LinkedList<Integer>>();
			  objInvIndex.unsortedOutputTermAndPostingList= new HashMap<String, LinkedList<Integer>>();
			  termsFromInputFile="";
			  termsFromInputFile+=line;
			  String[] totalInputTerms = termsFromInputFile.split(" ");
			  
			  for(int i=0;i<totalInputTerms.length;i++)
			  {
				  Iterator<String> keyIterator = objInvIndex.hmTermAndPostingList.keySet().iterator();
				  while(keyIterator.hasNext())
				  {
					  String term=keyIterator.next();
					  if(totalInputTerms[i].equalsIgnoreCase(term))
					  {
						  objInvIndex.hmOutputTermAndPostingList.put(term,objInvIndex.hmTermAndPostingList.get(term));
					  }
				  }
			  }
			  
			  for(int i=0;i<totalInputTerms.length;i++)
 			  {
				  Iterator<String> keyIterator = objInvIndex.unsortedTermAndPostingList.keySet().iterator();
				  while(keyIterator.hasNext())
				  {
					  String term=keyIterator.next();
					  if(totalInputTerms[i].equalsIgnoreCase(term))
					  {
						  objInvIndex.unsortedOutputTermAndPostingList.put(term,objInvIndex.unsortedTermAndPostingList.get(term));
					  }
				  }
			  }
			  
			 if(inputLine!=1)
				  writer.write("\n");
			  
			 String[] input_line = line.split(" ");
			  for(String word : input_line)
			  {
				for(String term : objInvIndex.hmTermAndPostingList.keySet())
				{
					if (term.equals(word))
					{    		
						List<Integer> postingList = new ArrayList<Integer>(); 
						postingList = objInvIndex.hmTermAndPostingList.get(term);
						writer.write("GetPostings\n" +term+ "\nPostings list: ");
						for(int doc : postingList)
						{
							writer.write(doc + " ");
						}
						writer.write("\n");
					}
				}
			  }
			  
			  //Logic for Term At A Time AND
			  writer.write("TaatAnd\n");
			  writer.write(line);
			  writer.write("\nResults: ");
			  LinkedList<Integer> l1= objInvIndex.getTAATAndPosting(objInvIndex.hmOutputTermAndPostingList,writer);
			  
			//Logic for Term At A Time OR
			  writer.write("TaatOr\n");
			  writer.write(line);
			  writer.write("\nResults: ");
			  TreeSet<Integer> l2= objInvIndex.getTAATOrPosting(objInvIndex.hmOutputTermAndPostingList,writer);
			  
			//Logic for Document At A Time AND
			  writer.write("DaatAnd\n");
			  writer.write(line);
			  writer.write("\nResults:");
			  TreeMap<Integer,Integer>hmDocIDAndCountAnd = objInvIndex.getDAATAndPosting(objInvIndex.hmTermAndPostingList,objInvIndex.hmOutputTermAndPostingList,writer,br,line,objInvIndex);
			  
			//Logic for Document At A Time OR
			  writer.write("\nDaatOr\n");
			  writer.write(line);
			  writer.write("\nResults:");
			  TreeMap<Integer,Integer>hmDocIDAndCountOr = objInvIndex.getDAATOrPosting(line,writer,br,objInvIndex);
		  }
		  writer.close();
		}
	  
	  public LinkedList<Integer> getTAATAndPosting(TreeMap<String, LinkedList<Integer>> hmTermAndPostingList,BufferedWriter writer) throws IOException
		{
		  	LinkedList<Integer> finalList = new LinkedList<Integer>();
		  	LinkedList<Integer> intersectionList = new LinkedList<Integer>();
		  	int count=0;
		  	int noOfComparisons=0;
		  	int countForFirstIteration=0;
		  	
		  	//If there is single word in input file
		  	if(hmTermAndPostingList.size()==1)
		  	{
		  		for(String list:hmTermAndPostingList.keySet())
		  		{
		  			LinkedList<Integer> lsposting=hmTermAndPostingList.get(list);
		  			finalList.addAll(lsposting);
		  			Iterator<Integer> itr=lsposting.iterator();
		  			while(itr.hasNext())
		  			{
		  				writer.write(itr.next()+" ");
		  			}
		  		}
		  		//writer.write(finalList.toString());
		  		writer.write("\nNumber of documents in results: 1");
				writer.write("\nNumber of comparisons: 0\n");
		  		
		  	}
		  	else
		  	{
		  		for(Map.Entry<String,LinkedList<Integer>> entry : hmTermAndPostingList.entrySet())
			  	{
			  		if(finalList.isEmpty())
				  	{
			  			countForFirstIteration++;
			  			finalList.addAll(entry.getValue());
			  			//noOfComparisons++; 
				  	}
			  			
			  		else
			  		{
			  			countForFirstIteration++;
			  			LinkedList<Integer> tempList= entry.getValue();
			  			intersectionList.clear();
			  			for(int iTempElement : tempList)
			  			{
			  				for(int iFinalElement : finalList)
				  			{
			  					//If there is a match in between the the intersection list and the new list
				  				if(iFinalElement == iTempElement)
				  				{
				  					intersectionList.add(iTempElement); //This contains the intersected list
				  					noOfComparisons++;
				  				}
				  				//Since the lists are sorted, we break the code if element of intersection list is greater than current list to optimize the code
				  				else if(iFinalElement>iTempElement)
				  				{
				  					noOfComparisons++;
				  					break;
				  				}
				  			}
			  			}
				  	}
			  		
			  		if(!intersectionList.isEmpty())
			  		{
			  			finalList.clear();
			  			finalList.addAll(intersectionList);
			  		}
			  		else if(intersectionList.isEmpty() && countForFirstIteration!=1)
			  		{
			  			finalList.clear();
			  		}
			  	}
			  	
			  if(intersectionList.isEmpty())
			  {
				  finalList.clear();
			  }
			  
			  if(finalList.isEmpty())
			  {
				  writer.write("empty");
				  writer.write("\nNumber of documents in results: "+finalList.size());
				  writer.write("\nNumber of comparisons: "+noOfComparisons+"\n");
			  }
			  else
			  {
				  for(int iIntersectionDocs : finalList)
				  {
					  writer.write(iIntersectionDocs+" ");
				  }
				  writer.write("\nNumber of documents in results: "+finalList.size());
				  writer.write("\nNumber of comparisons: "+noOfComparisons+"\n");
			  }
		   }
		  
		  return finalList;
		}
	  
	  @SuppressWarnings("unchecked")
	public TreeSet<Integer> getTAATOrPosting(TreeMap<String, LinkedList<Integer>> hmTermAndPostingList,BufferedWriter writer) throws IOException
		{
		  	Set<Integer> finalSet =  new HashSet<Integer>();
		  	Set<Integer> unionSet = new HashSet<Integer>();
		  	int count=0;
		  	int noOfComparisons=0;
		  	for(Map.Entry<String,LinkedList<Integer>> entry : hmTermAndPostingList.entrySet())
		  	{
		  		if(finalSet.isEmpty())
			  	{
		  			finalSet.addAll(new HashSet<Integer>(entry.getValue()));
		  			//TODO: check for this counter
		  			//noOfComparisons++;
			  	}
		  		else
		  		{
		  			Iterator<Integer> itrFinal= finalSet.iterator();
		  			Set<Integer> tempList= new HashSet<Integer>(entry.getValue());
		  			Iterator<Integer> itrTemp= tempList.iterator();
		  			int iFinalElement =0;
		  			int iTempElement=0;
		  			
		  			//This iterator contains the union result
		  			while(itrFinal.hasNext())
		  			{
		  				iFinalElement=itrFinal.next();
		  				//This iterator contains the current posting list to be compared
		  				while(itrTemp.hasNext())
		  				{
		  					iTempElement=itrTemp.next();
		  					//If the result is already present in the union, we do not insert the duplicate entries
		  					if(iFinalElement == iTempElement)
			  				{
		  						noOfComparisons++;
			  					//break;
			  				}
		  					
		  					//We add the new elements to the union list
		  					else if(iFinalElement!=iTempElement)
			  				{
			  					unionSet.add(iTempElement);//This contains the intersected list
			  					noOfComparisons++;
			  				}
		  				}
		  		    }
			  	}
		  		
		  		if(!unionSet.isEmpty())
		  		{
		  			finalSet.addAll(unionSet);
		  		}
		  	}
		  	
		  
		  TreeSet<Integer> finalTreeSet = new TreeSet<Integer>();
	      finalTreeSet.addAll(finalSet);
		  
		  if(finalTreeSet.isEmpty())
		  {
			  writer.write("empty");
			  writer.write("\nNumber of documents in results: "+finalTreeSet.size());
			  writer.write("\nNumber of comparisons: "+noOfComparisons+"\n");
		  }
		  else
		  {
			  for(int iIntersectionDocs : finalTreeSet)
			  {
				  writer.write(iIntersectionDocs+" ");
			  }
			  writer.write("\nNumber of documents in results: "+finalTreeSet.size());
			  writer.write("\nNumber of comparisons: "+noOfComparisons+"\n");
		  }
		  
		  return finalTreeSet;
		}
	  
	  private TreeMap<Integer,Integer> getDAATAndPosting(TreeMap<String, LinkedList<Integer>> hmTermAndPostingList,TreeMap<String, LinkedList<Integer>> hmOutputTermAndPostingList,BufferedWriter writer,BufferedReader br,String line,InvIndex objInvIndex) throws IOException
	  {
		  	ArrayList<Iterator<Integer>> itr = new ArrayList<Iterator<Integer>>();
		  	TreeMap<Integer,Integer> hmDocIDandCount = new TreeMap<Integer, Integer>();
	  		ArrayList<Integer[]> alPostingList = new ArrayList<Integer[]>();
	  		
	        int imaxPostingListSize = 0;
	        int count=0;
	        int docNo=0;
	        
	        String[] words = line.split(" ");

		  	for(Map.Entry<String, LinkedList<Integer>> entry: hmOutputTermAndPostingList.entrySet())
     		{
     			LinkedList<Integer> lsPostingList = entry.getValue();
     			itr.add(lsPostingList.iterator());
     			if(imaxPostingListSize<=lsPostingList.size())
     			{
     				imaxPostingListSize = lsPostingList.size();
     			}
     		}

		  	int itrSize=itr.size();
     		for(int i = 0;i<imaxPostingListSize;i++)
     		{
     			for(int j = 0;j<itr.size();j++)
     			{
     				if(itr.get(j).hasNext())
     				{
     					count++;
     					int countOfDoc =itr.get(j).next();
     					
     					//If the doc id is present if the treemap, increment the the count for that doc id 
     					if(objInvIndex.isTermPresent(hmDocIDandCount,countOfDoc))
     					{
     						hmDocIDandCount.put(countOfDoc, hmDocIDandCount.get(countOfDoc)+1);
     					}
     					//If the doc id is not present, add it to the treemap and enter its count as 1
     					else
                        {
                        	hmDocIDandCount.put(countOfDoc, 1);
                        }
     				}
     			}
     		}
     		//If the DAAT AND result is empty, write empty.
     		if(objInvIndex.isDAATANDResultEmpty(hmDocIDandCount,itrSize))
     		{
     			writer.write(" empty");
     		}
     		else
     		{
     			for(Entry<Integer,Integer> e :hmDocIDandCount.entrySet())
     			{
     				Integer[] temp = {e.getKey(),e.getValue()};
                    alPostingList.add(temp);
     			}
     		}

     		if(!alPostingList.isEmpty())
     		{
     			for(int i=0;i<alPostingList.size();i++)
     			{
     				if(alPostingList.get(i)[1]==words.length)
     				{
     					docNo++;
     					writer.write(" "+alPostingList.get(i)[0]);
     				}
     			}
     		 }
     		
     		writer.write("\nNumber of documents in results: "+docNo);
     		writer.write("\nNumber of comparisons: "+count);
     		return hmDocIDandCount;
	  }
	  
	  private TreeMap<Integer,Integer> getDAATOrPosting(String line,BufferedWriter writer,BufferedReader br,InvIndex objInvIndex) throws IOException 
	  {
		  	ArrayList<Iterator<Integer>> itr = new ArrayList<Iterator<Integer>>();
		  	TreeMap<Integer,Integer> hmDocIDandCount = new TreeMap<Integer, Integer>();
	  		
	  		int imaxPostingListSize = 0;
	        int count=0;
	        
	        for(Map.Entry<String, LinkedList<Integer>> entry: hmOutputTermAndPostingList.entrySet())
		  	{
	   			LinkedList<Integer> lsPostingList = entry.getValue();
	   			itr.add(lsPostingList.iterator());
	   			if(imaxPostingListSize<=lsPostingList.size())
	   			{
	   				imaxPostingListSize = lsPostingList.size();
	   			}
		  	}

		  	for(int i = 0;i<imaxPostingListSize;i++)
	   		{
	   			for(int j = 0;j<itr.size();j++)
	   			{
	   				if(itr.get(j).hasNext())
	   				{
	   					count++;
	   					int docIDCount =itr.get(j).next();
	   					
	   					//If the doc id is present if the treemap, increment the the count for that doc id 
	   					if(objInvIndex.isTermPresent(hmDocIDandCount,docIDCount))
	   					{
	   						hmDocIDandCount.put(docIDCount, hmDocIDandCount.get(docIDCount)+1);
	   					}
	   					//If the doc id is not present, add it to the treemap and enter its count as 1
	   					else
	                      {
	                      	hmDocIDandCount.put(docIDCount, 1);
	                      }
	   				}
	   			}
	   		}
		  
		if(hmDocIDandCount.isEmpty())
	  	{
	  		writer.write(" empty");
	  	}
	  	else
	  	{
	  		for(Entry<Integer,Integer> entry :hmDocIDandCount.entrySet())
	  		{
	  			writer.write(" "+entry.getKey());
	  		}
	  	}
	  	writer.write("\nNumber of documents in results: "+hmDocIDandCount.size());
	  	writer.write("\nNumber of comparisons: "+count);
	  	
	  	return hmDocIDandCount;
	  }
	  

	  public void generatePostingList(String text,Directory index,IndexReader reader) throws IOException
	  {
		  Terms terms = MultiFields.getTerms(reader,text);
		  TermsEnum termEnum=terms.iterator();
		  String dictionaryTerms="";

		  while (termEnum.next() != null )
		  {
			  BytesRef term = termEnum.term();
              dictionaryTerms=term.utf8ToString();
              PostingsEnum postingEnum = MultiFields.getTermDocsEnum(reader,text,term);
              LinkedList<Integer> postingList = new LinkedList<Integer>();
              while(postingEnum.nextDoc()!=PostingsEnum.NO_MORE_DOCS)
              {
            	  postingList.add(postingEnum.docID());
    		  }
              hmTermAndPostingList.put(dictionaryTerms, postingList);
              unsortedTermAndPostingList.put(dictionaryTerms, postingList);
              
          }
	  }
	  
	  public boolean isTermPresent(TreeMap<Integer,Integer>hmDocIDandCount,int k)
	  {
		  for(Map.Entry<Integer, Integer> entry :hmDocIDandCount.entrySet())
		  {
			  int docId=entry.getKey();
			  if(docId==k)
			  {
				  return true;
			  }
			}
		  return false;
	  }

	  public boolean isDAATANDResultEmpty(TreeMap<Integer,Integer> hmDocIDandCount,int itrSize)
	  {
		  for(Map.Entry<Integer, Integer> entry :hmDocIDandCount.entrySet())
		  {
			  int docCount=entry.getValue();
			  if(docCount==itrSize)
			  {
				  return false;
			  }
		  }
		  return true;
	  }
}