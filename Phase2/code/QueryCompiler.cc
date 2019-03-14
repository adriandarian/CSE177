#include "QueryCompiler.h"
#include "QueryOptimizer.h"
#include "Schema.h"
#include "ParseTree.h"
#include "Record.h"
#include "DBFile.h"
#include "Comparison.h"
#include "Function.h"
#include "RelOp.h"

using namespace std;


QueryCompiler::QueryCompiler(Catalog& _catalog, QueryOptimizer& _optimizer) :
	catalog(&_catalog), optimizer(&_optimizer) {
}

QueryCompiler::~QueryCompiler() {
}

void QueryCompiler::Compile(TableList* _tables, NameList* _attsToSelect, FuncOperator* _finalFunction, AndList* _predicate, NameList* _groupingAtts, int& _distinctAtts, QueryExecutionTree& _queryTree) {

	// create a SCAN operator for each table in the query
	int nTbl = 0;
	for (TableList* node = _tables; node != NULL; node = node->next) {
		nTbl += 1;
	}

	RelationalOp** forest = new RelationalOp*[nTbl];
	Schema* forestSchema = new Schema[nTbl];
	int idx = 0;
	for (TableList* node = _tables; node != NULL; node = node->next) {
		string s = node->tableName;
		bool b = catalog->GetSchema(s, forestSchema[idx]);
		if (false == b) {
			cout << "Semantic error: table " << s << " does not exist in the database!" << endl;
			exit(1);
		}

		DBFile dbFile;
		forest[idx] = new Scan(forestSchema[idx], dbFile, s);
		idx += 1;
	}
	
	for (int i = 0; i < nTbl; i++) {
		cout << *forest[i] << endl;
	}



	// push-down selections: create a SELECT operator wherever necessary
	for (int i = 0; i < nTbl; i++) {
		Record literal;
		CNF cnf;
		int ret = cnf.ExtractCNF(*_predicate, forestSchema[i], literal);
		if (0 != ret) {
			exit(1);
		}

		RelationalOp* op = new Select(forestSchema[i], cnf, literal, forest[i]);
		forest[i] = op;
	}

	for (int i = 0; i < nTbl; i++) {
		cout << *forest[i] << endl;
	}


	// call the optimizer to compute the join order
	OptimizationTree* root;
	optimizer->Optimize(_tables, _predicate, root);
	OptimizationTree* rootCopy = root;

	for (int i = 0; i < nTbl; i++) {
		cout << *forest[i] << endl;
	}



	// create join operators based on the optimal order computed by the optimizer
	RelationalOp* join = constTree(rootCopy, _predicate);

	for (int i = 0; i < nTbl; i++) {
		cout << *forest[i] << endl;
	}
	



	// create the remaining operators based on the query
	if (_finalFunction == NULL) {
		Schema projSch;
		join->returnSchema(projSch);
		vector<Attribute> projAtts = projSch.GetAtts();	

		NameList* attsToSelect = _attsToSelect;
		int numAttsInput = projSch.GetNumAtts(), numAttsOutput = 0; 
		Schema projSchOut = projSch;
		vector<int> keepMe;

		while (attsToSelect != NULL) {
			string str(attsToSelect->name);
			keepMe.push_back(projSch.Index(str));
			attsToSelect = attsToSelect->next;
			numAttsOutput++;
		}
		int* keepme = new int [keepMe.size()];
		for (int i = 0;i < keepMe.size(); i++) keepme[i] = keepMe[i]; 

		projSchOut.Project(keepMe);
		Project* project = new Project (projSch, projSchOut, numAttsInput, numAttsOutput, keepme, join);
	
		join = (RelationalOp*) project;

		if (_distinctAtts == 1) {
			Schema dupSch;	
			join->returnSchema(dupSch);	
			DuplicateRemoval* duplicateRemoval = new DuplicateRemoval(dupSch, join);
			join = (RelationalOp*) duplicateRemoval;
		}

	}	else {
		if (_groupingAtts == NULL) {
			Schema schIn, schIn_;
			join->returnSchema(schIn_);
			schIn = schIn_;

			Function compute;
			FuncOperator* finalFunction = _finalFunction;
			compute.GrowFromParseTree(finalFunction, schIn_);

			vector<string> attributes, attributeTypes;
			vector<unsigned int> distincts;
			attributes.push_back("Sum");
			attributeTypes.push_back("FLOAT");
			distincts.push_back(1);
			Schema schOutSum(attributes, attributeTypes, distincts);
	
			Sum* sum = new Sum (schIn, schOutSum, compute, join);
			join = (RelationalOp*) sum;
		} else {
			Schema schIn, schIn_;
			join->returnSchema(schIn_);
			schIn = schIn_;

			NameList* grouping = _groupingAtts;
			int numAtts = 0; 
			vector<int> keepMe;

			vector<string> attributes, attributeTypes;
			vector<unsigned int> distincts;
			attributes.push_back("Sum");
			attributeTypes.push_back("FLOAT");
			distincts.push_back(1);

			while (grouping != NULL) {
				string str(grouping->name);
				keepMe.push_back(schIn_.Index(str));
				attributes.push_back(str);

				Type type;
				type = schIn_.FindType(str);

				switch(type) {
					case Integer:	attributeTypes.push_back("INTEGER");	break;
					case Float:	attributeTypes.push_back("FLOAT");	break;
					case String:	attributeTypes.push_back("STRING");	break;
					default:	attributeTypes.push_back("UNKNOWN");	break;
				}
			
				distincts.push_back(schIn_.GetDistincts(str));
		
				grouping = grouping->next;
				numAtts++;
			}

			int* keepme = new int [keepMe.size()];
			for (int i = 0; i < keepMe.size(); i++) keepme[i] = keepMe[i];
		
			Schema schOut(attributes, attributeTypes, distincts);
			OrderMaker groupingAtts(schIn_, keepme, numAtts);

			Function compute;
			FuncOperator* finalFunction = _finalFunction;
			compute.GrowFromParseTree(finalFunction, schIn);

			GroupBy* groupBy = new GroupBy (schIn, schOut, groupingAtts, compute, join);	
			join = (RelationalOp*) groupBy;
		}	
	}
	Schema finalSchema;
	join->returnSchema(finalSchema);
	string outFile = "Output.txt";


	WriteOut* writeout = new WriteOut(finalSchema, outFile, join);
	join = (RelationalOp*) writeout;




	// connect everything in the query execution tree and return
	_queryTree.SetRoot(*join);	



	// free the memory occupied by the parse tree since it is not necessary anymore

}

RelationalOp* QueryCompiler::constTree(OptimizationTree* root, AndList* _predicate) {
	if (root->leftChild == NULL && root->rightChild == NULL) {	
		RelationalOp* op;
		return op;
	}

	if (root->leftChild->tables.size() == 1  && root->rightChild->tables.size() == 1) {
		string left = root->leftChild->tables[0];
		string right = root->rightChild->tables[0];

		CNF cnf;
		Schema schemaLeft, schemaRight;
		RelationalOp* leftOP, *rightOP;
	
		leftOP->returnSchema(schemaLeft);
		rightOP->returnSchema(schemaRight);
		
		cnf.ExtractCNF(*_predicate, schemaLeft, schemaRight);
		Schema schemaOut = schemaLeft;
		schemaOut.Append(schemaRight);
		Join* join = new Join(schemaLeft, schemaRight, schemaOut, cnf, leftOP, rightOP);
		return ((RelationalOp*) join);
	}

	if (root->leftChild->tables.size() == 1) {	
		string left = root->leftChild->tables[0];
		Schema schemaLeft, schemaRight;
		CNF cnf;		
		RelationalOp* leftOP;

		leftOP->returnSchema(schemaLeft);
		RelationalOp* rightOP = constTree(root->rightChild, _predicate);
		rightOP->returnSchema(schemaRight);

		cnf.ExtractCNF(*_predicate, schemaLeft, schemaRight);
		Schema schemaOut = schemaLeft;
		schemaOut.Append(schemaRight);
		Join* join = new Join(schemaLeft, schemaRight, schemaOut, cnf, leftOP , rightOP);
		return ((RelationalOp*) join);
	}

	if (root->rightChild->tables.size() == 1) {	
		string right = root->rightChild->tables[0];
		Schema schemaLeft, schemaRight;
		CNF cnf;
		RelationalOp* rightOP;

		rightOP->returnSchema(schemaRight);
		RelationalOp* leftOP = constTree(root->leftChild, _predicate);
		leftOP->returnSchema(schemaLeft);
		
		cnf.ExtractCNF(*_predicate, schemaLeft, schemaRight);
		Schema schemaOut = schemaLeft;
		schemaOut.Append(schemaRight);
		Join* join = new Join(schemaLeft, schemaRight, schemaOut, cnf, leftOP , rightOP);
		return ((RelationalOp*) join);
	}

	Schema schemaLeft,schemaRight;
	CNF cnf;
	RelationalOp* leftOP = constTree(root->leftChild, _predicate);
	RelationalOp* rightOP = constTree(root->rightChild, _predicate);

	leftOP->returnSchema(schemaLeft);
	rightOP->returnSchema(schemaRight);

	cnf.ExtractCNF(*_predicate, schemaLeft, schemaRight);
	Schema schemaOut = schemaLeft;
	schemaOut.Append(schemaRight);
	Join* join = new Join(schemaLeft, schemaRight, schemaOut, cnf, leftOP, rightOP);
	return ((RelationalOp*) join);

}

