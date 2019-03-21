#include <iostream>
#include <string>

#include "Catalog.h"
extern "C"{
#include "QueryParser.h"
}
#include "QueryOptimizer.h"
#include "QueryCompiler.h"
#include "RelOp.h"

using namespace std;


// these data structures hold the result of the parsing
extern struct FuncOperator* finalFunction; // the aggregate function
extern struct TableList* tables; // the list of tables in the query
extern struct AndList* predicate; // the predicate in WHERE
extern struct NameList* groupingAtts; // grouping attributes
extern struct NameList* attsToSelect; // the attributes in SELECT
extern int distinctAtts; // 1 if there is a DISTINCT in a non-aggregate query

extern "C" int yyparse();
extern "C" int yylex_destroy();



void printStructureData() {
	//first check what is in each of the structures and how the values from the parser are bgin stored

	//tables
	cout << "--------------------Printing tables list-------------------------------\n";
	TableList* tempTables;
	tempTables = tables;
	while (tempTables != NULL) {
		cout << tempTables->tableName << endl;
		tempTables = tempTables->next;
	}

	//predicates
	cout << "--------------------Printing predicates list-------------------------------\n";
	AndList* tempPredicate;
	tempPredicate = predicate;
	string a;
	while (tempPredicate != NULL) {
		if (tempPredicate->left->code == 5) {
			a = "<";
		}
		else if (tempPredicate->left->code == 6) {
			a = ">";
		}
		else if (tempPredicate->left->code == 7) {
			a = "=";
		}
		cout << tempPredicate->left->left->value << " " << a << " " << tempPredicate->left->right->value << endl;
		tempPredicate = tempPredicate->rightAnd;
	}

	//grouping attributes
	cout << "--------------------Printing grouping by list-------------------------------\n";
	NameList* tempGroupingAtts;
	tempGroupingAtts = groupingAtts;
	while (tempGroupingAtts != NULL) {
		cout << tempGroupingAtts->name << endl;
		tempGroupingAtts = tempGroupingAtts->next;
	}

	// Select attributes
	cout << "--------------------Printing select clause list-------------------------------\n";
	NameList* tempAttsToSelect;
	tempAttsToSelect = attsToSelect;
	while (tempAttsToSelect != NULL) {
		cout << tempAttsToSelect->name << endl;
		tempAttsToSelect = tempAttsToSelect->next;
	}
	// Aggregate function
	cout << "--------------------Printing aggregate function-------------------------------\n";
	FuncOperator* tempFinalFunction;
	tempFinalFunction = finalFunction;
	while (tempFinalFunction != NULL) {
		cout << tempFinalFunction->leftOperand->code << " " << tempFinalFunction->leftOperand->value << endl;
		tempFinalFunction = tempFinalFunction->right;
	}
}


void getAllCurrentTables(Catalog & catalog, vector<string>&currentTables) {
	vector<string>catalogTablesList;
	catalog.GetTables(catalogTablesList);
	TableList* tempTables;
	tempTables = tables;

	while (tempTables != NULL) {
		for (int i = 0; i < catalogTablesList.size(); i++) {
			if (catalogTablesList[i] == tempTables->tableName) {
				
				currentTables.push_back(tempTables->tableName);
			}

		}

		tempTables = tempTables->next;

	}

}
bool checkIfTablesExists(Catalog &catalog) {
	vector<string>catalogTablesList;
	catalog.GetTables(catalogTablesList);
	TableList* tempTables;
	tempTables = tables;
	while (tempTables != NULL) {
		bool found = false;
		for (int i = 0; i < catalogTablesList.size(); i++) {
			if (catalogTablesList[i] == tempTables->tableName) {
				cout << "SUCCESS TABLE: " << tempTables->tableName << " WAS FOUND" << endl;
				found = true;
			}
			
		}
		if (!found) {
			cout << "FAILURE TABLE: " << tempTables->tableName << " WAS NOT FOUND" << endl;
			return false;
		}
		
		tempTables = tempTables->next;
		
	}
	return true;

}
bool checkIfPredicatesExists(Catalog &catalog) {
	vector<string>currentTables;
	vector<string>attributes;
	getAllCurrentTables(catalog, currentTables);
	AndList* tempPredicate;
	tempPredicate = predicate;

	for (int i = 0; i < currentTables.size();  i++) {
			catalog.GetAttributes(currentTables[i], attributes);
	}
	while (tempPredicate != NULL) {
		bool found = false;
		bool found2 = false;

		if (tempPredicate->left->left->code != tempPredicate->left->right->code) {

			string attributeName;
			//check which one of the sides is the attribute
			if (tempPredicate->left->left->code == NAME) {
				attributeName = tempPredicate->left->left->value;
			}
			else {
				attributeName = tempPredicate->left->right->value;
			}

			for (int j = 0; j < attributes.size(); j++) {

				if (attributes[j] == attributeName) {
					cout << "SUCCESS ATTRIBUTE: " << attributeName << " WAS FOUND IN THE DATABASE" << endl;
					found = true;
				}

			}
			if (!found) {
				cout << "FAILURE ATTRIBUTE: " << attributeName << " WAS NOT FOUND" << endl;
				return false;
			}
		}

		else if (tempPredicate->left->left->code == tempPredicate->left->right->code) {

			for (int j = 0; j < attributes.size(); j++) {

				if (attributes[j] == tempPredicate->left->left->value) {
					cout << "SUCCESS LEFT ATTRIBUTE: " << tempPredicate->left->left->value << " WAS FOUND IN THE DATABASE" << endl;
					found = true;
				}
				if (attributes[j] == tempPredicate->left->right->value) {
					cout << "SUCCESS RIGHT ATTRIBUTE: " << tempPredicate->left->right->value << " WAS FOUND IN THE DATABASE" << endl;
					found2 = true;
				}

			}
			if (!found) {
				cout << "FAILURE ATTRIBUTE: " << tempPredicate->left->left->value << " WAS NOT FOUND" << endl;
				return false;
			}
			if (!found2) {
				cout << "FAILURE ATTRIBUTE: " << tempPredicate->left->right->value << " WAS NOT FOUND" << endl;
				return false;
			}
		}

		tempPredicate = tempPredicate->rightAnd;

	}
	
	return true;

}
bool checkIfGroupingAttsExists(Catalog&catalog) {
	vector<string>currentTables;
	vector<string>attributes;
	getAllCurrentTables(catalog, currentTables);
	NameList* tempGroupAtts;
	tempGroupAtts = groupingAtts;

	for (int i = 0; i < currentTables.size(); i++) {
		catalog.GetAttributes(currentTables[i], attributes);
	}
	while (tempGroupAtts != NULL) {
		bool found = false;
		for (int j = 0; j < attributes.size(); j++) {

			if (attributes[j] == tempGroupAtts->name) {
				cout << "SUCCESS ATTRIBUTE: " << tempGroupAtts->name << " WAS FOUND IN THE DATABASE" << endl;
				found = true;
			}

		}

		if (!found) {
			cout << "FAILURE ATTRIBUTE: " << tempGroupAtts->name << " WAS NOT FOUND" << endl;
			return false;
		}

		tempGroupAtts = tempGroupAtts->next;

	}
	return true;
}
bool checkIfSelectAttsExists(Catalog&catalog) {
	vector<string>currentTables;
	vector<string>attributes;
	getAllCurrentTables(catalog, currentTables);
	NameList* tempSelectAtts;
	tempSelectAtts = attsToSelect;

	for (int i = 0; i < currentTables.size(); i++) {
		catalog.GetAttributes(currentTables[i], attributes);
	}
	while (tempSelectAtts != NULL) {
		bool found = false;
		for (int j = 0; j < attributes.size(); j++) {

			if (attributes[j] == tempSelectAtts->name) {
				cout << "SUCCESS ATTRIBUTE: " << tempSelectAtts->name << " WAS FOUND IN THE DATABASE" << endl;
				found = true;
			}

		}

		if (!found) {
			cout << "FAILURE ATTRIBUTE: " << tempSelectAtts->name << " WAS NOT FOUND" << endl;
			return false;
		}

		tempSelectAtts = tempSelectAtts->next;

	}
	return true;
}
bool checkIfFunctionAttsExists(Catalog&catalog) {
	vector<string>currentTables;
	vector<string>attributes;
	getAllCurrentTables(catalog, currentTables);
	FuncOperator* tempFunctionAtts;
	tempFunctionAtts = finalFunction;

	for (int i = 0; i < currentTables.size(); i++) {
		catalog.GetAttributes(currentTables[i], attributes);
	}
	while (tempFunctionAtts != NULL) {

		bool found = false;
		bool found2 = false;

		if (tempFunctionAtts->code > 0 && tempFunctionAtts->code < 100) {

			for (int j = 0; j < attributes.size(); j++) {

				if (attributes[j] == tempFunctionAtts->leftOperator->leftOperand->value) {
					cout << "SUCCESS ATTRIBUTE: " << tempFunctionAtts->leftOperator->leftOperand->value << " WAS FOUND IN THE DATABASE" << endl;
					found = true;
				}
				if (attributes[j] == tempFunctionAtts->right->leftOperand->value) {
					cout << "SUCCESS ATTRIBUTE: " << tempFunctionAtts->right->leftOperand->value << " WAS FOUND IN THE DATABASE" << endl;
					found2 = true;
				}

			}
			if (!found) {
				cout << "FAILURE ATTRIBUTE: " << tempFunctionAtts->leftOperator->leftOperand->value << " WAS NOT FOUND" << endl;
				return false;
			}
			if (!found2) {
				cout << "FAILURE ATTRIBUTE: " << tempFunctionAtts->right->leftOperand->value << " WAS NOT FOUND" << endl;
				return false;
			}

			tempFunctionAtts = tempFunctionAtts->right->right;

		}

		else {

			for (int j = 0; j < attributes.size(); j++) {

				if (attributes[j] == tempFunctionAtts->leftOperand->value) {
					cout << "SUCCESS ATTRIBUTE: " << tempFunctionAtts->leftOperand->value << " WAS FOUND IN THE DATABASE" << endl;
					found = true;
				}

			}
			if (!found) {
				cout << "FAILURE ATTRIBUTE: " << tempFunctionAtts->leftOperand->value << " WAS NOT FOUND" << endl;
				return false;
			}

			tempFunctionAtts = tempFunctionAtts->right;

		}

	}
	return true;
}

int main () {
	string dbFile = "catalog.sqlite";
	Catalog catalog(dbFile);
	//cout << catalog << endl;
	// this is the query optimizer
	// it is not invoked directly but rather passed to the query compiler
	QueryOptimizer optimizer(catalog);

	// this is the query compiler
	// it includes the catalog and the query optimizer
	QueryCompiler compiler(catalog, optimizer);


	// the query parser is accessed directly through yyparse
	// this populates the extern data structures
	int parse = -1;
	
	if (yyparse () == 0) {
		cout << "OK!" << endl;

		//checkIfAttributesExists();
		parse = 0;
		
	}
	else {
		cout << "Error: Query is not correct!" << endl;
		parse = -1;
	}

	//this is just a function for printing out what is inside the variables for creating our tree
	//printStructureData();

	//checkIfTablesExists(catalog);
	yylex_destroy();

	if (parse != 0) return -1;

	// at this point we have the parse tree in the ParseTree data structures
	// we are ready to invoke the query compiler with the given query
	// the result is the execution tree built from the parse tree and optimized
	if (!checkIfTablesExists(catalog) || !checkIfPredicatesExists(catalog) || !checkIfGroupingAttsExists(catalog) || !checkIfSelectAttsExists(catalog) || !checkIfFunctionAttsExists(catalog)) {
		cout << "Query is INVALID" << endl;
		return 1;
	}
	QueryExecutionTree queryTree;
	compiler.Compile(tables, attsToSelect, finalFunction, predicate,
		groupingAtts, distinctAtts, queryTree);

	
	cout << queryTree << endl;
	
	return 0;
}
