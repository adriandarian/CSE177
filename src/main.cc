#include <iostream>
#include <string>

#include "Catalog.h"
extern "C"
{
#include "QueryParser.h"
}
#include "QueryOptimizer.h"
#include "QueryCompiler.h"
#include "RelOp.h"

using namespace std;

// these data structures hold the result of the parsing
extern struct FuncOperator *finalFunction; // the aggregate function
extern struct TableList *tables;					 // the list of tables in the query
extern struct AndList *predicate;					 // the predicate in WHERE
extern struct NameList *groupingAtts;			 // grouping attributes
extern struct NameList *attsToSelect;			 // the attributes in SELECT
extern int distinctAtts;									 // 1 if there is a DISTINCT in a non-aggregate query

extern "C" int yyparse();
extern "C" int yylex_destroy();

void menu(QueryCompiler &compiler)
{
}

int main()
{
	// this is the catalog
	string dbFile = "catalog.sqlite";
	Catalog catalog(dbFile);

	int n;
	cout << "\nWhat would you like to do?\n";
	cout << "\t1. Look at the schema for the catalog.\n";
	cout << "\t2. Run a query.\n";
	cout << "\t3. Drop a table from the catalog.\n";
	cout << "\t4. Exit.\n";
	cin >> n;

	switch (n)
	{
	case 1:
	{
		cout << catalog << endl;
		break;
	}
	default:
	case 2:
	{
		char m;
		cout << "Would you like to display the Query Execution Tree with the output? (y/n)\n";
		cin >> m;

		// this is the query optimizer
		// it is not invoked directly but rather passed to the query compiler
		QueryOptimizer optimizer(catalog);

		// this is the query compiler
		// it includes the catalog and the query optimizer
		QueryCompiler compiler(catalog, optimizer);
		// bool on = true;
		// while(on){
		// 	string s;
		// 	cout << "Input a query? Yes or Exit";
		// 	cin >> s;

		// 	if(s != "exit"){
		// the query parser is accessed directly through yyparse
		// this populates the extern data structures

		int parse = -1;
		if (yyparse() == 0)
		{
			//cout << "OK!" << endl;
			parse = 0;
		}
		else
		{
			cout << "Error: Query is not correct!\n";
			parse = -1;
		}

		yylex_destroy();

		if (parse != 0)
			return -1;

		// at this point we have the parse tree in the ParseTree data structures
		// we are ready to invoke the query compiler with the given query
		// the result is the execution tree built from the parse tree and optimized
		if (parse == 0)
		{
			QueryExecutionTree queryTree;
			compiler.Compile(tables, attsToSelect, finalFunction, predicate,
											 groupingAtts, distinctAtts, queryTree);

			if (m == 'y' || m == 'Y')
				cout << queryTree << endl;

			queryTree.ExecuteQuery();
		}
		break;
	}
	case 3:
	{
		cout << "\nwhich table do you want to drop? ";
		string t;
		cin >> t;
		catalog.DropTable(t);
		catalog.Save();
		break;
	}
	case 4:
	{
		return 0;
	}
	}

	return 0;
}