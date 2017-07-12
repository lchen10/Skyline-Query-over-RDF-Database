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

public class GenerateSkyline3Dim {

	/**
	 * @param args
	 */
	
	static final String txtfilename = "anti-0.5m.txt";
	static final String rdffilename = "Anti-0.5m.rdf";


	static final String custUri = "http://customers/";
	static final String custProUri = "http://customers/property/";

	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub

		// List<Statement> stmtList = new ArrayList<Statement>();
		// create an empty model
		// Model model = ModelFactory.createDefaultModel();

		// create property
		// Property HeightIs = model.createProperty(custProUri + "HeightIs");
		// Property ageIs = model.createProperty(custProUri + "AgeIs");
		// Property incomeIs = model.createProperty(custProUri + "IncomeIs");

		// List<Resource> custResList = new ArrayList<Resource>();

		BufferedReader br = new BufferedReader(new FileReader(txtfilename));

//		Model model = null;
//		Property ageIs = null;
//		Property incomeIs = null;
		List<Statement> stmtList = null;

		String s;
		br.readLine();
		int count = 1;

		Model model = ModelFactory.createDefaultModel();
		Property ageIs = model.createProperty(custProUri + "AgeIs");
		Property incomeIs = model.createProperty(custProUri + "IncomeIs");
		Property HeightIs = model.createProperty(custProUri + "HeightIs");
		
		stmtList = new ArrayList<Statement>();
		while ((s = br.readLine()) != null) {
			String data[] = s.split(" ");

			Integer col1data = (int) (Double.parseDouble(data[0]) * 1000000);
			Integer col2data = (int) (Double.parseDouble(data[1]) * 1000000);
			Integer col3data = (int) (Double.parseDouble(data[2]) * 1000000);

			String col1String = col1data.toString();
			String col2String = col2data.toString();
			String col3String = col3data.toString();

			Resource cust = model.createResource(custUri + "cust" + count);

			// Statement s1 = model.createStatement(cust, HeightIs,
			// heightString);
			Statement s1 = model.createStatement(cust, ageIs, col1String);
			Statement s2 = model.createStatement(cust, incomeIs, col2String);
			Statement s3 = model.createStatement(cust, HeightIs, col3String);
			// stmtList.add(s1);
			stmtList.add(s1);
			stmtList.add(s2);
			stmtList.add(s3);

			// custResList.add(cust);
			System.out.println("insert the " + count
					+ " statement successfully!!");
			count++;

		}
		
		model.add(stmtList);
		OutputStream out = new FileOutputStream(rdffilename);
		model.write(out, "N-TRIPLE");
		System.out.println("Generate rdf " + rdffilename 
				+ " successfully!");

	}

}
