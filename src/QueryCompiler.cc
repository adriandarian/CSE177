#include "QueryCompiler.h"
#include "QueryOptimizer.h"
#include "Schema.h"
#include "ParseTree.h"
#include "Record.h"
#include "DBFile.h"
#include "Comparison.h"
#include "Function.h"
#include "RelOp.h"
#include <vector>
#include <iostream>
#include <string.h>

using namespace std;

QueryCompiler::QueryCompiler(Catalog &_catalog, QueryOptimizer &_optimizer) : catalog(&_catalog), optimizer(&_optimizer)
{
	//catalog->print();
}

QueryCompiler::~QueryCompiler()
{
	for (int i = 0; i < everything.size(); i++)
	{
		delete everything[i];
	}
}

void QueryCompiler::Compile(TableList *_tables, NameList *_attsToSelect,
														FuncOperator *_finalFunction, AndList *_predicate,
														NameList *_groupingAtts, int &_distinctAtts,
														QueryExecutionTree &_queryTree)
{
	// create a SCAN operator for each table in the query
	string file;
	TableList *temp = _tables;
	while (temp != NULL)
	{
		Schema schema;
		string name = temp->tableName;
		//Check if table exists
		if (!catalog->GetDataFile(name, file))
		{
			exit(-1);
		}
		//cout << file << endl;
		//Check if tables exists
		if (!catalog->GetSchema(name, schema))
		{
			exit(-1);
		}
		DBFile dFile;
		//for now just cheese
		char *cfile = new char[file.length() + 1];
		strcpy(cfile, file.c_str());
		if (dFile.Open(cfile) == -1)
		{
			cout << cfile << endl;
			exit(-1);
		}
		scans.push_back(new Scan(schema, dFile));
		scNames.push_back(name);
		scSchemas.push_back(schema);
		everything.push_back(scans[scans.size() - 1]);
		temp = temp->next;
	}
	// for(int i = 0; i < scans.size();i++){
	// 	cout << scans[i] << " [" << scNames[i] << "]"<<endl;
	// }

	//---------------------------------------------------------------ERROR CHECKING---------------------------------------------------
	//Schema used to debug
	Schema all = scSchemas[0];
	//Append every schema to make one large schema we can look through
	for (int k = 1; k < scans.size(); k++)
	{
		all.Append(scSchemas[k]);
	}
	//Get the attributes
	//vector<Attribute>attAll = all.GetAtts();
	//Error checking for attributes in the SELECT CLAUSE
	NameList *aerr = _attsToSelect;
	while (aerr != NULL)
	{
		string aName = aerr->name;
		//cout << aName << endl;
		//If the attribute name isn't in the vector of all attributes, there is a problem.
		if (all.Index(aName) == -1)
		{
			cout << aName << " is not found in the database" << endl;
			exit(-1);
		}
		aerr = aerr->next;
	}
	AndList *ptemp = _predicate;
	string lval;
	string rval;
	int lcode;
	int rcode;
	//Error Checking for predicates, ie stuff in the WHERE clause
	while (ptemp != NULL)
	{
		bool foundL = false;
		bool foundR = false;

		if (ptemp->left != NULL)
		{
			//Super confusing make it shorter
			lval = ptemp->left->left->value;
			rval = ptemp->left->right->value;
			lcode = ptemp->left->left->code;
			rcode = ptemp->left->right->code;
			//cout << "Left value :" << lval << " Right value: " << rval << endl;

			if (lcode == NAME)
			{
				if (all.Index(lval) == -1)
				{
					cout << lval << " is not found in the database" << endl;
					exit(-1);
				}
				// for(int l = 0; l < attAll.size();l++){
				//  	if(lval == attAll[l].name){
				//  		foundL = true;
				//  	}
				// }
				// if(foundL == false){
				// 	cout << lval << " not found in the schema!"<< endl;
				// 	exit(-1);
				// }
			}

			if (rcode == NAME)
			{
				if (all.Index(rval) == -1)
				{
					cout << rval << " is not found in the database" << endl;
					exit(-1);
				}
				// for(int l = 0; l < attAll.size();l++){
				//  	if(rval == attAll[l].name){
				//  		foundR = true;
				//  	}
				// }
				// if(foundR == false){
				// 	cout << rval << " not found in the schema!"<< endl;
				// 	exit(-1);
				// }
			}
		}
		ptemp = ptemp->rightAnd;
	}

	//--------------------------------------------------PUSH-DOWN SELECTS------------------------------------------------------

	// push-down selections: create a SELECT operator wherever necessary
	// only happen with literals use ExtractCNF(...literal)
	//For every scan, extract a CNF
	for (int i = 0; i < scans.size(); i++)
	{
		//scout << scNames[i] << endl;
		if (_predicate != NULL)
		{

			// for(int i = 0; i < schema.GetNumAtts();i++){
			// 	cout << schema.GetAtts()[i].name << endl;
			// }
			CNF cnf;
			Record rec; //Where the constants will be written to.
			if (cnf.ExtractCNF(*_predicate, scSchemas[i], rec) == -1)
			{
				//cout << "die" << endl;
				//die = true;
				exit(-1);
			}

			if (cnf.numAnds > 0)
			{
				//Push the selects to vector, use the corresponding scan
				selects.push_back(new Select(scSchemas[i], cnf, rec, scans[i]));
				indSS.push_back(i); //Store matching scan index so we can access later
				everything.push_back(selects[selects.size() - 1]);
			}
		}
	}

	//---------------------------------------------------------------------JOIN ORDER--------------------------------------------
	// call the optimizer to compute the join order
	OptimizationTree root;
	optimizer->Optimize(_tables, _predicate, &root);
	//optimizer->print();
	OptimizationTree *iter = &root;
	OptimizationTree *left;
	//create join operators based on the optimal order computed by the optimizer

	//Go down to the very left child to start the build
	while (iter != NULL)
	{
		//cout << iter->size << endl;
		left = iter;
		iter = iter->leftChild;
	}
	//cout << "Check Point 1" << endl;
	//Create tree using post order traversal
	OptimizationTree *right;
	OptimizationTree *parent = left->parent;
	if (parent != NULL)
	{
		right = left->parent->rightChild;
	}
	if (parent != NULL && right != NULL)
	{
		//Value to see how far we push the joins
		int push = 0;
		while (left->parent != NULL)
		{
			//cout << left->size << endl;
			//Make the cnf so we can join later
			CNF cnf;
			if (cnf.ExtractCNF(*_predicate, left->schema, right->schema) == -1)
			{
				exit(-1);
			}
			//The two producers, either scans, selects or another join
			RelationalOp *rL;
			RelationalOp *rR;
			//The first join of the tree, the very beginning only
			if (left->tables.size() == 1)
			{
				//Theres only one table since it hasn't been joined yet, look for it in the vector of schemas corresponding to scans
				for (int i = 0; i < scans.size(); i++)
				{
					//If they match then set the left operator equal to the scan;
					if (left->tables[0] == scNames[i])
					{
						rL = scans[i];
						//Check for a select made for that scan, look for a matching index in the indSS vector and if found,set the left operator equal to it
						for (int k = 0; k < selects.size(); k++)
						{
							if (i == indSS[k])
							{
								rL = selects[k];
							}
						}
					}
				}
			}
			else
			{
				//This happens every other time, there have been joins made previously
				// for(int i = 0; i < joins.size();i++){
				// 	//Check for sizes of the tables, the size of jNames should be the same as left->tables since they are both the latest version
				// 	if(left->tables.size() == jNames[i].size()){
				// 		rL = joins[i];
				// 	}
				// }
				//The latest join is always going to be the next left producer
				rL = joins[joins.size() - 1];
			}
			//The right side will always be a scan or a select
			for (int i = 0; i < scans.size(); i++)
			{
				//Only one table so we just match it to a scan
				if (right->tables[0] == scNames[i])
				{
					// cout << scNames[i];
					// cout << i << endl;
					//Do same as before, look for a select.
					rR = scans[i];
					for (int k = 0; k < selects.size(); k++)
					{
						if (i == indSS[k])
						{
							rR = selects[k];
						}
					}
				}
			}
			// cout << "left: ";
			// for(int i = 0; i < left->schema.GetAtts().size();i++){
			// 	cout << left->schema.GetAtts()[i].name;
			// 	if(i != left->schema.GetAtts().size()-1){
			// 		cout << ", ";
			// 	}
			// }
			// cout << endl;
			// cout << "right: ";
			// for(int i = 0; i < right->schema.GetAtts().size();i++){
			// 	cout << right->schema.GetAtts()[i].name;
			// 	if(i != right->schema.GetAtts().size()-1){
			// 		cout << ", ";
			// 	}
			// }
			// cout << endl;
			// cout << "parent: ";
			// for(int i = 0; i < parent->schema.GetAtts().size();i++){
			// 	cout << parent->schema.GetAtts()[i].name;
			// 	if(i != parent->schema.GetAtts().size()-1){
			// 		cout << ", ";
			// 	}
			// }
			// cout << endl;

			//Make joins and add them to the vector, the left schema , right schema and parent schema of the optimization tree are used
			joins.push_back(new Join(left->schema, right->schema, parent->schema, cnf, rL, rR));
			//Set join size to size of the parent
			joins[joins.size() - 1]->size = parent->size;
			//Push the names of all the tables
			jNames.push_back(parent->tables);
			//Push the schema
			jSchemas.push_back(parent->schema);
			//Increment thorugh the tree
			left = left->parent;
			//Make a check since we need to increment these too
			if (left != NULL && left->parent != NULL)
			{
				parent = left->parent;
				right = parent->rightChild;
			}
			//Increment the push
			push++;

			everything.push_back(joins[joins.size() - 1]);
		}
		//Calculate the push for each join so we can print nicely
		for (int i = 0; i < joins.size(); i++)
		{
			joins[i]->push = push;
			push--;
		}
	}
	//cout << "Check Point 2" << endl;
	// create the remaining operators based on the query

	//The latest schema from the join orders/selects/scans
	Schema in;
	//The parse tree pointer
	RelationalOp *rP;
	//Check if there were any joins at all, if not then we only build from scans or selects
	if (joins.size() > 0)
	{
		//In is the most recent join's schema
		in = jSchemas[joins.size() - 1];
		//rP points to the most recent join
		rP = joins[joins.size() - 1];
	}
	else
	{
		//Check if there were any selects made, it wil either be 1 or 0 since anything greater will have forced a join
		if (selects.size() == 1)
		{
			//Point to the select
			rP = selects[0];
		}
		else
		{
			//Point to a scan if there were no selects made
			rP = scans[0];
		}
		//in = the only schema
		in = scSchemas[0];
	}
	// NameList* atemp = _attsToSelect;
	// vector<Attribute> aIn = in.GetAtts();
	// while(atemp != NULL){
	// 	string aName = atemp->name;
	// 	//cout << aName << endl;
	// 	if(in.Index(aName)==-1){
	// 		cout << aName << " is not found in the database" << endl;
	// 		die = true;
	// 		exit(-1);
	// 	}
	// 	atemp = atemp->next;
	// }

	if (_groupingAtts == NULL)
	{
		//SELECT-FROM-WHERE, appened project at root, if theres a distinct, append that at root further in.
		if (_finalFunction == NULL)
		{
			//The schema we will be sending to WRITEOUT
			Schema out = in;
			//The number of attributes of the schema in
			int input = in.GetNumAtts();
			int output = 0;
			//A vector of indexes for attributes being selected
			vector<int> keep;
			//A reversal of those indexes so we can print them in the right order
			vector<int> bkeep;
			//The attributes of the schema in
			vector<Attribute> a = in.GetAtts();
			//Traverese through the attributes and input their indexes if they are in the selected attributes
			NameList *atemp = _attsToSelect;
			while (atemp != NULL)
			{
				for (int i = 0; i < a.size(); i++)
				{
					if (a[i].name == atemp->name)
					{
						keep.push_back(i);
						//Keep track of the number of values in the atts to select
						output++;
						//cout << atemp->name << " "<< i <<endl;
					}
				}
				atemp = atemp->next;
			}
			// //Why is it backwards
			// for(int i = 0; i < keep.size();i++){
			// 	cout << keep[i] << endl;
			// }
			//Store the reversed indexes so we can get the correct order
			for (int i = keep.size() - 1; i >= 0; i--)
			{
				bkeep.push_back(keep[i]);
			}
			//Keep the selected attributes
			out.Project(bkeep);
			//Make an array using the indexes of bkeep
			int *keepMe = new int[bkeep.size()];
			for (int i = 0; i < bkeep.size(); i++)
			{
				keepMe[i] = bkeep[i];
				//cout << bkeep[i]<< " "<< keepMe[i] << endl;
			}
			//Using that array, the number of attributes, the in and out schema make a project, rP is the latest join/select/scan
			Project *p = new Project(in, out, input, output, keepMe, rP);
			everything.push_back(p);
			//Also check if there was a distinct
			if (_distinctAtts > 0)
			{
				//If yes, set the writeOut schema to out
				wOut = out;
				//Make this with the same schema and the project as the producer
				DuplicateRemoval *dR = new DuplicateRemoval(out, p);
				everything.push_back(dR);
				//Set the tree to equal to dR since its the last producer
				rP = dR;
			}
			else
			{
				//Otherwise, point rP to p since that was the last producer made
				//the latest schema to wOut
				rP = p;
				wOut = out;
			}
		}
		//Sum operator, inserted at root
		else
		{
			//Use these vectors to make to make a new schema
			vector<string> atts;
			vector<string> types;
			vector<unsigned int> dist;
			//Grow a function from the _finalFunction
			Function func;
			func.GrowFromParseTree(_finalFunction, in);
			//Going to push this string as the attribute
			string s = "sum(";
			//Iterate through _finalFunction to get all the values
			FuncOperator *ftemp = _finalFunction;

			//int f = 0;
			while (ftemp != NULL)
			{
				//cout << s << endl;
				//Check for a leftOperator and get its leftOperand
				if (ftemp->leftOperator != NULL)
				{
					s += ftemp->leftOperator->leftOperand->value;
				}
				//Check if this has a value
				else if (ftemp->leftOperand != NULL)
				{
					s += ftemp->leftOperand->value;
					//cout << ftemp->leftOperand->value << endl;
					//cout << "findex: " << f << endl;
				}
				else
				{
					//cout << "NULL " << "findex: " << f << endl;
				}
				//If there is a right add the code to the string
				if (ftemp->right != NULL)
				{
					s += " ";
					s += ftemp->code;
					s += " ";
				}
				//Incrment through the _finalFunction
				ftemp = ftemp->right;
				//f++;
			}
			//cout << f << endl;
			s += ")";
			//cout << s << endl;
			//Push back the string
			atts.push_back(s);
			//Push back the type
			types.push_back(func.getType());
			//Push back 1 since we only have one result from the function
			dist.push_back(1);
			//Make a schema from the vectors
			Schema out(atts, types, dist);
			//Create a Sum
			Sum *sum = new Sum(in, out, func, rP);

			everything.push_back(sum);
			//Set the writeOut schema to out
			wOut = out;
			//Point the next tree pointer to sum
			rP = sum;
		}
	}
	else
	{
		//If there are grouping values

		//Vectors of the attributes in the correct order of the query
		vector<string> atts;
		vector<string> types;
		vector<unsigned> dist;
		//Vectors of the attributes given by _groupingAtts
		vector<string> ratts;
		vector<string> rtypes;
		vector<unsigned> rdist;
		//Vector to keep track of the indexes
		vector<int> gAtts;
		//Number of _groupingAtts
		int gNum = 0;
		//Temp pointer
		NameList *gTemp = _groupingAtts;
		//Iterate through
		while (gTemp != NULL)
		{
			//Check for the name
			string name = gTemp->name;
			//cout << name << endl;
			int nDist = in.GetDistincts(name);
			//Not found
			if (nDist == -1)
			{
				cout << name << " not found" << endl;
				exit(-1);
			}
			//cout << name << endl;
			//Push them back into the arrays
			ratts.push_back(name);
			rdist.push_back(nDist);
			//Get the types of the attribute
			Type aType = in.FindType(name);
			//Push back the appropiate type
			if (aType == Integer)
			{
				rtypes.push_back("INTEGER");
			}
			else if (aType == Float)
			{
				rtypes.push_back("FLOAT");
			}
			else if (aType == String)
			{
				rtypes.push_back("STRING");
			}
			//Push back the index of the attribute in the schema
			gAtts.push_back(in.Index(name));
			//Increase number of attributes
			gNum++;

			gTemp = gTemp->next;
		}
		//Get attributes as before, and add them to the reversed vectors
		NameList *atemp2 = _attsToSelect;
		vector<Attribute> aIn2 = in.GetAtts();
		while (atemp2 != NULL)
		{
			string aName = atemp2->name;
			Type aType = in.FindType(aName);
			int nDist = in.GetDistincts(aName);
			if (nDist == -1)
			{
				cout << aName << " not found" << endl;
				exit(-1);
			}
			ratts.push_back(aName);
			rdist.push_back(nDist);
			if (aType == Integer)
			{
				rtypes.push_back("INTEGER");
			}
			else if (aType == Float)
			{
				rtypes.push_back("FLOAT");
			}
			else if (aType == String)
			{
				rtypes.push_back("STRING");
			}
			atemp2 = atemp2->next;
		}
		//If there is a function do the same as before
		Function func;
		FuncOperator *ftemp = _finalFunction;

		if (_finalFunction != NULL)
		{
			func.GrowFromParseTree(_finalFunction, in);
			string s = "sum(";
			//int f = 0;
			while (ftemp != NULL)
			{
				//cout << s << endl;
				if (ftemp->leftOperator != NULL)
				{
					s += ftemp->leftOperator->leftOperand->value;
				}
				else if (ftemp->leftOperand != NULL)
				{
					s += ftemp->leftOperand->value;
					//cout << ftemp->leftOperand->value << endl;
					//cout << "findex: " << f << endl;
				}
				else
				{
					//cout << "NULL " << "findex: " << f << endl;
				}
				if (ftemp->right != NULL)
				{
					s += " ";
					s += ftemp->code;
					s += " ";
				}
				ftemp = ftemp->right;
				//f++;
			}
			//cout << f << endl;
			s += ")";
			//cout << s << endl;
			ratts.push_back(s);
			rtypes.push_back(func.getType());
			rdist.push_back(1);
		}
		//Reverse the attributes
		for (int i = ratts.size() - 1; i > 0; i--)
		{
			atts.push_back(ratts[i]);
			types.push_back(rtypes[i]);
			dist.push_back(rdist[i]);
		}

		//Make a schema
		Schema out(atts, types, dist);
		int *ats = new int[gAtts.size()];
		for (int i = 0; i < gAtts.size(); i++)
		{
			ats[i] = gAtts[i];
		}
		//Make an order
		OrderMaker ord(in, ats, gNum);
		//And create a group by

		Function *function = new Function();
		function->GrowFromParseTree(_finalFunction, in);
		//GroupBy * groupBy = new GroupBy(schemaTmp, schemaTmpOut, orderMaker, *function, _finalFunction, producerLast);

		// GroupBy *gB = new GroupBy(in, out, ord, *function, _finalFunction, rP);
		GroupBy *gB = new GroupBy(in, out, ord, *function, rP);

		//Set the tree
		rP = gB;
		//Set the schema
		wOut = out;

		everything.push_back(gB);
	}

	//cout << "Check Point 3" << endl;

	// connect everything in the query execution tree and return
	//The final step
	string wat = "wat.out"; // Bogus string
	WriteOut *writeOut = new WriteOut(wOut, wat, rP);
	rP = writeOut;
	_queryTree.SetRoot(*rP);

	// free the memory occupied by the parse tree since it is not necessary anymore
}
