#include "QueryCompiler.h"
#include "QueryOptimizer.h"
#include "Schema.h"
#include "ParseTree.h"
#include "Record.h"
#include "DBFile.h"
#include "Comparison.h"
#include "Function.h"
#include "RelOp.h"
#include <string.h>
using namespace std;


QueryCompiler::QueryCompiler(Catalog& _catalog, QueryOptimizer& _optimizer) :
	catalog(&_catalog), optimizer(&_optimizer) {
}

QueryCompiler::~QueryCompiler() {
}

void QueryCompiler::Compile(TableList* _tables, NameList* _attsToSelect,
	FuncOperator* _finalFunction, AndList* _predicate,
	NameList* _groupingAtts, int& _distinctAtts,
	QueryExecutionTree& _queryTree) {

	unordered_map<string, RelationalOp*> PD_SEL;
	TableList* tblList = _tables;

	while (_tables != NULL) {
		DBFile db;
		Schema schema;

		string tblName = (string)_tables->tableName;

		if (!catalog->GetSchema(tblName, schema)) {
			cout << "ERROR: Table " << tblName << " does NOT exist" << endl;
			exit(-1);
		}
		string dbFilePath;		
		catalog->GetDataFile(tblName, dbFilePath);
		char* dbFilePathC = new char[dbFilePath.length()+1]; 
//		dbFilePathC = dbFilePathC.c_str();
		strcpy(dbFilePathC, dbFilePath.c_str());
		if(db.Open(dbFilePathC) == -1) {
			// error message is already shown in File::Open
			exit(-1);
		}

		// create a SCAN operator for each table in the query
		Scan* scan = new Scan(schema, db);
		scan->tableName = tblName;

		// stores scans
		PD_SEL[tblName] = (RelationalOp*) scan;

		//retrieve predicate from table
		if (_predicate != NULL) {
			CNF predicate;
			Record literal;

			// ExtractCNF: success = 0, fail = -1
			predicate.ExtractCNF(*_predicate, schema, literal);

			// as long as number of comparsion is greater than 0
			if (predicate.numAnds > 0) {
				// push-down selections: create a SELECT operator wherever necessary
				Select* select = new Select(schema, predicate, literal, (RelationalOp*) scan);

				//store selects
				PD_SEL[tblName] = (RelationalOp*) select;
			}
		} else {
			cout << "Format is NOT one of the following: SELECT-FROM-WHERE or SELECT-FROM-WHERE-GROUPBY!" << endl; 
		}

		// go to next table in TableList
		_tables = _tables->next;
	}
	

	// call the optimizer to compute the join order
	OptimizationTree root;
	optimizer->Optimize(tblList, _predicate, &root);
	OptimizationTree* rootTree = &root;

	//printPostorder(rootTree);

	// create join operators based on the optimal order computed by the optimizer
	RelationalOp* executionTree = BJT(rootTree, _predicate, PD_SEL);

	// create the remaining operators based on the query
	RelationalOp* executionTreeRoot = (RelationalOp*) executionTree;

	// grabs input from the join tree
	Schema inputSchema = executionTree->getSchema();

	// GRAMMER:
	// CASE 1: SELECT-FROM-WHERE
	if (_groupingAtts == NULL) {
		if (_finalFunction == NULL) {
			vector<int> reverseSavedAttributes;
			vector<Attribute> attributes = inputSchema.GetAtts();
			int outputNumAttribute = 0;

			int inputNumAttribute = inputSchema.GetNumAtts();
			Schema outputSchema = inputSchema;

			while (_attsToSelect != NULL) {
				string attributeName;
				attributeName = (string)_attsToSelect->name;

				for (int i = 0; i < attributes.size(); i++) {
					if (attributeName == attributes[i].name) {
						reverseSavedAttributes.push_back(i);
						outputNumAttribute++;
						break;
					}
				}

				// go to next name in NameList
				_attsToSelect = _attsToSelect->next;
			}

			vector<int> savedAttributes;
			for (int i = reverseSavedAttributes.size()-1; i >= 0; i--) {
				savedAttributes.push_back(reverseSavedAttributes[i]);
			}
			
			
			// project attribute of a schema, fail = -1, otherwise = 0
			outputSchema.Project(savedAttributes);

			//make a vector?????? OR THIS NOT NEEDED AND PASS vector savedAttributes
			int* saveAtts = new int[savedAttributes.size()];
			for (int i = 0; i < savedAttributes.size(); i++) {
				saveAtts[i] = savedAttributes[i];
			}

			Project* project = new Project(inputSchema, outputSchema, inputNumAttribute, outputNumAttribute, saveAtts, executionTree);

			if (_distinctAtts == 0) {
				executionTreeRoot = (RelationalOp*) project;
			} else {
				Schema inputSchema = project->getSchema();
				DuplicateRemoval* distinct = new DuplicateRemoval(inputSchema, project);
				executionTreeRoot = (RelationalOp*) distinct;
			}
		} else {
			FuncOperator* finalFunction = _finalFunction;
			string functionStr = appendSumAttributes(finalFunction);

			vector<string> atts;
			vector<string> attsTypes;
			vector<unsigned int> distincts;
			string dataType = "SUM: ";

			Function calculate;
			calculate.GrowFromParseTree(_finalFunction, inputSchema);

			if (calculate.getReturnsInt() == 1) {
				attsTypes.push_back("INTEGER");
				dataType += "INTEGER";
			} else {
				attsTypes.push_back("FLOAT");
				dataType += "FLOAT";
			}

			atts.push_back(dataType);
			distincts.push_back(1);
			Schema outputSchema(atts, attsTypes, distincts);

			Sum* sum = new Sum(inputSchema, outputSchema, calculate, executionTree);
			sum->expression = functionStr;
			executionTreeRoot = (RelationalOp*) sum;
		}
	// CASE 2: SELECT-FROM-WHERE-GROUPBY
	} else {
		vector<string> reverseAttributes;
		vector<string> reverseAttributeTypes;
		vector<unsigned int> reverseDistincts;
		vector<int> reverseGroupingAttributes;
		int numGroupingAttributes = 0;

		while (_groupingAtts != NULL) {
			string attributeName;
			attributeName = (string)_groupingAtts->name;
			
			int numDistinct;
			numDistinct = inputSchema.GetDistincts(attributeName);

			if (numDistinct == -1) {
				cout << "ERROR: Attribute " << attributeName << " does NOT exist!" << endl;
				exit(-1);
			}

			Type attributeType = inputSchema.FindType(attributeName);
			string attributeTypeString;

			if (attributeType == String) {
				attributeTypeString = "STIRNG";
			} else if (attributeType == Integer) {
				attributeTypeString = "INTEGER";
			} else if (attributeType == Float) {
				attributeTypeString = "FLOAT";
			} else {
				attributeTypeString = "N/A";
			}

			reverseAttributes.push_back(attributeName);
			reverseAttributeTypes.push_back(attributeTypeString);
			reverseDistincts.push_back(numDistinct);
			reverseGroupingAttributes.push_back(inputSchema.Index(attributeName));
			numGroupingAttributes++;

			// go to nexting grouping attribute in NameList
			_groupingAtts = _groupingAtts->next;
		}

		string atts = "SUM:";
		Function calculate;
		if (_finalFunction != NULL) {
			calculate.GrowFromParseTree(_finalFunction, inputSchema);


			if (calculate.getReturnsInt() == 1) {
		 		reverseAttributeTypes.push_back("INTEGER");
				atts += "INTEGER";
			} else {
		 		reverseAttributeTypes.push_back("FLOAT");
				atts += "FLOAT";
			}
			reverseAttributes.push_back(atts);
			reverseDistincts.push_back(1);
		}


		// reversing vectors
		vector<string> attributes;
		for (int i = reverseAttributes.size()-1; i >= 0; i--) {
			attributes.push_back(reverseAttributes[i]);
		}
		
		vector<string> attributeTypes;
		for (int i = reverseAttributeTypes.size()-1; i >= 0; i--) {
			attributeTypes.push_back(reverseAttributeTypes[i]);
		}
		
		vector<unsigned int> distincts;
		for (int i = reverseDistincts.size()-1; i >= 0; i--) {
			distincts.push_back(reverseDistincts[i]);
		}
		
		vector<int> groupingAttributes;
		for (int i = reverseGroupingAttributes.size()-1; i >= 0; i--) {
			groupingAttributes.push_back(reverseGroupingAttributes[i]);
		}

		Schema outputSchema(attributes, attributeTypes, distincts);
		int *attributeOrder = new int [groupingAttributes.size()];
		for (int i = 0; i < groupingAttributes.size(); i++) {
				attributeOrder[i] = groupingAttributes[i];
		}

		OrderMaker orderingAttributes(inputSchema, attributeOrder, numGroupingAttributes);
		GroupBy* groupBy = new GroupBy(inputSchema, outputSchema, orderingAttributes, calculate, executionTree);
		executionTreeRoot = (RelationalOp*) groupBy;
	}

	// connect everything in the query execution tree and return
	string outputFile = "3.out";
	Schema outputSchema = executionTreeRoot->getSchema();
	WriteOut* writeOut = new WriteOut(outputSchema, outputFile, executionTreeRoot);
	executionTreeRoot = (RelationalOp*) writeOut;

	_queryTree.SetRoot(*executionTreeRoot);
	// free the memory occupied by the parse tree since it is not necessary anymore
	_tables = NULL;
	_attsToSelect = NULL;
	_finalFunction = NULL;
	_predicate = NULL;
	_groupingAtts = NULL;
}

// a recursive function to create Join operators (w/ Select & Scan) from optimization result
RelationalOp* QueryCompiler::BJT(OptimizationTree*& tree, AndList* _predicate, unordered_map<string, RelationalOp*>& PD_SEL) {
	if (tree->leftChild != NULL && tree->rightChild != NULL) {
		RelationalOp* leftRelOp = BJT(tree->leftChild, _predicate, PD_SEL);
		RelationalOp* rightRelOp = BJT(tree->rightChild, _predicate, PD_SEL);

		Schema leftSchema;
		leftSchema = leftRelOp->getSchema();

		Schema rightSchema;
		rightSchema = rightRelOp->getSchema();

		CNF predicate;
		predicate.ExtractCNF(*_predicate, leftSchema, rightSchema);

		Schema outputSchema;
		outputSchema.Append(leftSchema);
		outputSchema.Append(rightSchema);
		Join* join = new Join(leftSchema, rightSchema, outputSchema, predicate, leftRelOp, rightRelOp);

		return (RelationalOp*) join;
	} else {
		return PD_SEL.find(tree->tables[0])->second;
	}
}

string QueryCompiler::appendSumAttributes(FuncOperator* str) {
	if (str->leftOperator == NULL && str->right == NULL) {
		return str->leftOperand->value;
	} else {
		string outputString;
		string leftStr = appendSumAttributes(str->leftOperator);
		string rightStr = appendSumAttributes(str->right);

		char* codeChar = (char*)&str->code;
		string codeStr = string(codeChar);
		outputString.append(leftStr);
		outputString.append(codeStr);
		outputString.append(rightStr);

		return outputString;
	}
}

void QueryCompiler::printPostOrder(OptimizationTree* node) 
{ 
    if (node == NULL) 
        return; 
  
    // first recur on left subtree 
    printPostOrder(node->leftChild); 
  
    // then recur on right subtree 
    printPostOrder(node->rightChild); 
  
    // now deal with the node 
	cout << "POSTORDER AREA: "<< endl;
    for (int i = 0; i < node->tables.size(); i++) {
		cout << node->tables[i] << ", ";
	}
	cout << endl;
} 