package Dim3;


import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.rdf.model.Property;
import com.hp.hpl.jena.rdf.model.RDFNode;
import com.hp.hpl.jena.rdf.model.Resource;
import com.hp.hpl.jena.rdf.model.Statement;
import com.hp.hpl.jena.rdf.model.StmtIterator;
import com.hp.hpl.jena.util.FileManager;

public class SkylineGenerateMultiPhase {

	static final String txtFileName = "normal-5m.txt";
	static final String generalRdfFileName = "Normal-5m";
	
	/**
	 * @param args
	 */

	static final String custUri = "http://customers/";
	static final String custProUri = "http://customers/property/";

	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub

		BufferedReader br = new BufferedReader(new FileReader(
				txtFileName));
		
		
		Model model = null;
		Property ageIs = null;
		Property incomeIs = null;
		Property heightIs = null;
		List<Statement> stmtList = null;
		
		String s;
		br.readLine();
		int count = 1;
		int subFileSize = 333334;
		int index = 1;
		int custNbr = 0;
		boolean needToAdd = false;
		
		while ((s = br.readLine()) != null) {
					
			if (count == 1)
			{
				model = ModelFactory.createDefaultModel();
				ageIs = model.createProperty(custProUri + "AgeIs");
				incomeIs = model.createProperty(custProUri + "IncomeIs");
				heightIs = model.createProperty(custProUri + "HeightIs");
				stmtList = new ArrayList<Statement>();
				needToAdd = false; 
				
			}

			String data[] = s.split(" ");
//			System.out.print("col1 is: " + data[0].substring(2) + " ");
//			System.out.print("col2 is: " + data[1] + " ");
//			System.out.println("col3 is: " + data[2]);
			Integer col1data = (int) (Double.parseDouble(data[0]) * 1000000);
			Integer col2data = (int) (Double.parseDouble(data[1]) * 1000000);
			Integer col3data = (int) (Double.parseDouble(data[2]) * 1000000);
			//System.out.print("col1 is: " + col1data + " ");
			//System.out.print("col2 is: " + col2data + " ");
			//System.out.println("col3 is: " + col3data);

			String col1String = col1data.toString();
			String col2String = col2data.toString();
			String col3String = col3data.toString();
			
			
			custNbr = count + subFileSize * (index-1);
			Resource cust = model.createResource(custUri + "cust" + custNbr);

			// Statement s1 = model.createStatement(cust, HeightIs,
			// heightString);
			Statement s1 = model.createStatement(cust, ageIs, col1String);
			Statement s2 = model.createStatement(cust, incomeIs, col2String);
			Statement s3 = model.createStatement(cust, heightIs, col3String);
			
			stmtList.add(s1);
			stmtList.add(s2);
			stmtList.add(s3);

//			custResList.add(cust);
			needToAdd = true;
			System.out.println("insert the " + count
					+ " statement successfully!!");
			count++;
			if (count > subFileSize)
			{
				count = 1;
				String filename = generalRdfFileName + "-sub-" + index + ".rdf";
				index++;
				model.add(stmtList);
				OutputStream out = new FileOutputStream(filename);
				model.write(out, "N-TRIPLE");
				needToAdd = false;
				System.out.println("Generate rdf " + filename + " of size " + subFileSize + " successfully!");
				
			}

		}

		if(needToAdd){
				count = 1;
				String filename = generalRdfFileName + "-sub-" + index + ".rdf";
				index++;
				model.add(stmtList);
				OutputStream out = new FileOutputStream(filename);
				model.write(out, "N-TRIPLE");
				needToAdd = false;
				System.out.println("Generate rdf " + filename + " of size " + subFileSize + " successfully!");
				
			
		}

		
		
		
		
		

	}

}
