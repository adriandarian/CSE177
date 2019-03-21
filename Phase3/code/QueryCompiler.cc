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

RelationalOp* getRelationalOperator(string tableName, vector<Scan*>allTableScans, vector<Select*>allSelects) {
	//check for the selects first since selects are connected to the scans then if its not found then you can look through the scans
	for (int i = 0; i < allSelects.size(); i++) {
		if (tableName == allSelects[i]->getTableName()) {
			//cout << allSelects[i]->getSchema() << endl;
			return allSelects[i];
		}
	}
	for (int i = 0; i < allTableScans.size(); i++) {
		//cout << tableName.compare(allTableScans[i]->getTableName()) << endl;
		if (tableName == allTableScans[i]->getTableName()) { 
			//cout << allTableScans[i]->getSchema() << endl;
			return allTableScans[i];
		}
	}
	
}

RelationalOp* postOrderTraversal(OptimizationTree*root, vector<Scan*>allTableScans, vector<Select*>allSelects, AndList* _predicate) {

	RelationalOp* leftChild;
	RelationalOp* rightChild;
	
	if (root->leftChild != NULL) {
		leftChild = postOrderTraversal(root->leftChild, allTableScans, allSelects, _predicate);
	}
	if (root->rightChild != NULL) {
		rightChild = postOrderTraversal(root->rightChild, allTableScans, allSelects, _predicate);
	}
	//cout << "CURRENT NODE ID: " << root->id << endl;
	


	if (root->tables.size() == 1) {
		//cout << "CURRENT NODE NUMBER OF TABLES: " << root->tables.size() << endl;
		return getRelationalOperator(root->tables[0], allTableScans, allSelects);
	}
	else {
		//cout << "CURRENT NODE NUMBER OF TABLES: " << root->tables.size() << endl;
		Schema leftSchema;
		Schema rightSchema;
		Schema outSchema;
		CNF tableCNF;

		Join* leftJoinCast = dynamic_cast<Join*>(leftChild);
		Join* rightJoinCast = dynamic_cast<Join*>(rightChild);

		Select* leftSelectCast = dynamic_cast<Select*>(leftChild);
		Select* rightSelectCast = dynamic_cast<Select*>(rightChild);

		Scan* leftScanCast = dynamic_cast<Scan*>(leftChild);
		Scan* rightScanCast = dynamic_cast<Scan*>(rightChild);

		//check left child what type it is: join, select, or scan
		if (leftJoinCast) {
			leftSchema = leftJoinCast->getSchemaOut();
		}
		else if (leftSelectCast) {
			leftSchema = leftSelectCast->getSchema();
		}
		else if (leftScanCast){
			leftSchema = leftScanCast->getSchema();
		}
		//check right child what type it is
		if (rightJoinCast) {
			rightSchema = rightJoinCast->getSchemaOut();
		}
		else if (rightSelectCast) {
			rightSchema = rightSelectCast->getSchema();
		}
		else if (rightScanCast) {
			rightSchema = rightScanCast->getSchema();
		}

		outSchema.Append(leftSchema);
		outSchema.Append(rightSchema);

		tableCNF.ExtractCNF(*_predicate, leftSchema, rightSchema);

		Join *joinedStuff = new Join(leftSchema, rightSchema, outSchema, tableCNF, leftChild, rightChild);
	
		return joinedStuff;

	}
	
	//leftSchema = getTableSchema(root->leftChild->tables[0]);
	
	//Join(Schema& _schemaLeft, Schema& _schemaRight, Schema& _schemaOut, 
	//CNF& _predicate, RelationalOp* _left, RelationalOp* _right);
	
}

void preOrderTraversal(RelationalOp* root, int count) {

	if (root != NULL) {
		for (int i = 0; i < count; i++) {
			cout << "-----";
		}
		cout << *root;
	}

	Join* joinCast = dynamic_cast<Join*>(root);
	

	Select* selectCast = dynamic_cast<Select*>(root);


	Scan* scanCast = dynamic_cast<Scan*>(root);

	Project * projectCast = dynamic_cast<Project*>(root);

	Sum * sumCast = dynamic_cast<Sum*>(root);

	GroupBy * groupByCast = dynamic_cast<GroupBy*>(root);

	DuplicateRemoval * distinctCast = dynamic_cast<DuplicateRemoval*>(root);

	WriteOut * writeOutCast = dynamic_cast<WriteOut*>(root);



	//check left child what type it is: join, select, or scan
	if (joinCast) {
		preOrderTraversal(joinCast->getLeftRelationalOp(), count + 1);
		preOrderTraversal(joinCast->getRightRelationalOp(), count + 1);
	}
	else if (selectCast) {
		preOrderTraversal(selectCast->getProducer(), count + 1);

	}
	else if (scanCast) {
		//cout << "There is no cast for SCAN" << endl;

	}
	else if (projectCast) {
		preOrderTraversal(projectCast->getProducer(), count + 1);
	}
	else if (sumCast) {
		preOrderTraversal(sumCast->getProducer(), count + 1);
	}
	else if (groupByCast) {
		preOrderTraversal(groupByCast->getProducer(), count + 1);
	}
	else if (distinctCast) {
		preOrderTraversal(distinctCast->getProducer(), count + 1);
	}
	else if (writeOutCast) {
		preOrderTraversal(writeOutCast->getProducer(), count + 1);
	}
	
	
}

string QueryCompiler::getPredicateTable(string _predicateValue) {
	vector<string>allTables;
	string predicateTableName;
	catalog->GetTables(allTables);
	for (int i = 0; i < allTables.size(); i++) {
		Schema tableSchema;
		catalog->GetSchema(allTables[i], tableSchema);
		int tableAttributeSize = tableSchema.GetNumAtts();
		for (int j = 0; j < tableAttributeSize; j++) {
			if (tableSchema.GetAtts()[j].name == _predicateValue) {
				predicateTableName = allTables[i];
			}
		}
	}

	return predicateTableName;
}

string convertType(Type oldType)
{
	string newType;
	//cout << oldType << endl;
	switch (oldType) {
	case String:
		newType = "STRING";
		break;
	case Integer:
		newType = "INTEGER";
		break;
	case Float:
		newType = "FLOAT";
		break;
	default:
		newType = "UNKNOWN";
		break;
	}

	return newType;
}

void QueryCompiler::Compile(TableList* _tables, NameList* _attsToSelect,
	FuncOperator* _finalFunction, AndList* _predicate,
	NameList* _groupingAtts, int& _distinctAtts,
	QueryExecutionTree& _queryTree) {

	// create a SCAN operator for each table in the query
	TableList* tempTables;
	tempTables = _tables;

	vector<Scan*>allTableScans;
	vector<Select*>allSelects;
	while (tempTables != NULL) {
		Schema tableSchema;
		string tableName = tempTables->tableName;
		DBFile dataFile = DBFile();

		catalog->GetSchema(tableName, tableSchema);
		

		Scan *tableScan = new Scan(tableSchema, dataFile, tableName);

		// push-down selections: create a SELECT operator wherever necessary
		CNF tableCNF;
		Record tableRecord;

		tableCNF.ExtractCNF(*_predicate, tableSchema, tableRecord);

		int CNFValues;
		CNFValues = tableCNF.numAnds;
		
		if (CNFValues != 0) {
			Select *selectTables = new Select(tableSchema, tableCNF, tableRecord, tableScan, tableName);
			allSelects.push_back(selectTables);
		}
		else {
			allTableScans.push_back(tableScan);
		}
		
		tempTables = tempTables->next;
	}

	// call the optimizer to compute the join order
	OptimizationTree* root = new OptimizationTree();
	optimizer->Optimize(_tables, _predicate, root);
	// create join operators based on the optimal order computed by the optimizer
	// - Traverse through the tree, and for each node make a Join RelOp. -
	// - Don't do this for the inital nodes (tables size of 1), since they are not joins, they are scans -
	
	RelationalOp* rootJoinRelationalOp;

	rootJoinRelationalOp = postOrderTraversal(root,allTableScans, allSelects, _predicate);
	
	// create the remaining operators based on the query

	// - Project -
	Join* joinCast = dynamic_cast<Join*>(rootJoinRelationalOp);
	Select* selectCast = dynamic_cast<Select*>(rootJoinRelationalOp);
	Scan* scanCast = dynamic_cast<Scan*>(rootJoinRelationalOp);
	
	Schema schemaIn;
	
	if (joinCast) {
		schemaIn = joinCast->getSchemaOut();
	}
	else if (selectCast) {
		schemaIn = selectCast->getSchema();
	}
	else if (scanCast) {
		schemaIn = scanCast->getSchema();
	}
	

	struct NameList* tempAttsToSelect;
	tempAttsToSelect = _attsToSelect;

	struct FuncOperator* tempFuncAtts;
	tempFuncAtts = _finalFunction;

	vector<Attribute> myAttributeInputs;
	vector<string> attributes;
	vector<string> attributeTypes;
	vector<unsigned int> distincts;
	
	myAttributeInputs = schemaIn.GetAtts();

	while (tempFuncAtts != NULL) {
		string attributeName1;
		string attributeName2;
		string attributeType1;
		string attributeType2;
		string attributeTypeToPushBack;
		bool found = false;
		bool found2 = false;
		string code;
		if (tempFuncAtts->code > 0 && tempFuncAtts->code < 100) {
			if (tempFuncAtts->code == 42) {
				code = " * ";
			}
			if (tempFuncAtts->code == 43) {
				code = " + ";
			}
			if (tempFuncAtts->code == 45) {
				code = " - ";
			}
			if (tempFuncAtts->code == 47) {
				code = " / ";
			}

			for (int i = 0; i < myAttributeInputs.size(); i++) {

				if (tempFuncAtts->leftOperator->leftOperand->value == myAttributeInputs[i].name) {
					attributeName1 = myAttributeInputs[i].name;
					attributeType1 = convertType(myAttributeInputs[i].type);
					found = true;
				}
				if (tempFuncAtts->right->leftOperand->value == myAttributeInputs[i].name) {
					attributeName2 = myAttributeInputs[i].name;
					attributeType2 = convertType(myAttributeInputs[i].type);
				
					found2 = true;
				}
				
			}
			if (found && found2) {
				attributes.push_back(attributeName1 + code + attributeName2);
				distincts.push_back(0);
				if (attributeType1 == attributeType2) {
					attributeTypes.push_back(attributeType1);
				}
				else if(attributeType1 != attributeType2) {
					if (attributeType1 == "FLOAT") {
						attributeTypeToPushBack = attributeType1;
					}
					else {
						attributeTypeToPushBack = attributeType2;
					}
					attributeTypes.push_back(attributeTypeToPushBack);
				}
				
			}
			tempFuncAtts = tempFuncAtts->right->right;

		}

		else {
			//cout << tempFuncAtts->leftOperand->value << endl;
			for (int i = 0; i < myAttributeInputs.size(); i++) {

				if (tempFuncAtts->leftOperand->value == myAttributeInputs[i].name) {
					attributes.push_back(myAttributeInputs[i].name);
					attributeTypes.push_back(convertType(myAttributeInputs[i].type));
					//distincts.push_back(0);
					distincts.push_back(myAttributeInputs[i].noDistinct);
				}
			}

			tempFuncAtts = tempFuncAtts->right;

		}

	}

	while (tempAttsToSelect != NULL) {
		//cout << tempAttsToSelect->name << endl;
		for (int i = 0; i < myAttributeInputs.size(); i++) {

			if (tempAttsToSelect->name == myAttributeInputs[i].name) {
				attributes.push_back(myAttributeInputs[i].name);
				attributeTypes.push_back(convertType(myAttributeInputs[i].type));
				distincts.push_back(myAttributeInputs[i].noDistinct);
			}
		}

		tempAttsToSelect = tempAttsToSelect->next;
	
	}

	int numAttsInput;
	int numAttsOutput;
	Schema schemaOut(attributes, attributeTypes, distincts);
	int * keepMe;

	numAttsInput = schemaIn.GetNumAtts();
	numAttsOutput = schemaOut.GetNumAtts();

	RelationalOp* secondLastRelOp;

	// - Project -

	if (_groupingAtts == NULL && _finalFunction == NULL) {
		Project *projectTable = new Project(schemaIn, schemaOut, numAttsInput, numAttsOutput, keepMe, rootJoinRelationalOp);
		secondLastRelOp = projectTable;
	}

	// - Sum -

	else if (_groupingAtts == NULL && _finalFunction != NULL) {
		Function compute;
		// I have no idea if this is needed for proj 2. Included it for now.
		// compute.GrowFromParseTree(_finalFunction, schemaOut)
		Sum *sumRelOp = new Sum(schemaIn, schemaOut, compute, rootJoinRelationalOp);
		secondLastRelOp = sumRelOp;
	}

	// - GroupBy -
	else if (_groupingAtts != NULL) {
		OrderMaker attsToGroup;
		// I have no idea if we need to use the constructor with the Schema parameter. (Get the schema for the atts that are on the group by).
		// I think the default constructor will worked fine.

		Function compute;
		// I have no idea if this is needed for proj 2. Included it for now.
		// NOTE, for groupby, only execute the line below IF there is a _finalFunction. (Check if _finalFunction is NULL or not). Can't growFromParseTree if _finalFunction is NULL.
		// compute.GrowFromParseTree(_finalFunction, schemaOut)
		GroupBy *groupByRealOp = new GroupBy(schemaIn, schemaOut, attsToGroup, compute, rootJoinRelationalOp);
		secondLastRelOp = groupByRealOp;
	}


	// - 2nd Last One: Duplicate Removal -
	// - Last One: Write Out -
	WriteOut* writeOutRelOp;
	string outFile = "queryOutput.txt";
	if (_distinctAtts == 1) {
		DuplicateRemoval * distincts = new DuplicateRemoval(schemaOut, secondLastRelOp);
		writeOutRelOp = new WriteOut(schemaOut, outFile, distincts);
	}

	// - Last One: Write Out -

	else {
		writeOutRelOp = new WriteOut(schemaOut, outFile, secondLastRelOp);
	}

	preOrderTraversal(writeOutRelOp, 0);

	if (_groupingAtts == NULL) {

		cout << "No grouping atts" << endl;

	}

	if (_finalFunction == NULL) {

		cout << "No Sum Function" << endl;

	}

	// connect everything in the query execution tree and return

	// free the memory occupied by the parse tree since it is not necessary anymore
}
