#include <iostream>
#include <string>
#include <vector>
#include <map>

#include "Catalog.h"
extern "C" {
#include "QueryParser.h"
}
#include "QueryOptimizer.h"
#include "QueryCompiler.h"
#include "Record.h"
#include "RelOp.h"
#include "Comparison.h"
#include "EfficientMap.h"
#include "EfficientMap.cc"

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


int main () {
	// while(1){	
		// this is the catalog
		string dbFile = "catalog.sqlite";
		Catalog catalog(dbFile);

		// int n;
		// cout<<endl<<"What would you like to do?"<<endl;
		// cout<<"1. Look at schema of all tables in catalog."<<endl;
		// cout<<"2. Run a query."<<endl;
		// cout<<"3. Drop a table from the catalog."<<endl;
		// cout<<"4. Exit."<<endl;
		// cin>>n;
		
		// switch(n){
		// 	case 1: cout<<catalog; break;
		// 	case 2: {
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
					parse = 0;
				} else {
					cout << "Error: Query is not correct!" << endl;
					parse = -1;
				}

				yylex_destroy();

				if (parse != 0) {
					return -1;
				}

				// at this point we have the parse tree in the ParseTree data structures
				// we are ready to invoke the query compiler with the given query
				// the result is the execution tree built from the parse tree and optimized
				QueryExecutionTree queryTree;
				compiler.Compile(tables, attsToSelect, finalFunction, predicate,
					groupingAtts, distinctAtts, queryTree);

				cout << queryTree << endl;
	// 			break;
	// 		}
	// 		case 3: {
	// 			cout<<endl<<"which table do you want to drop? ";
	// 			string t;
	// 			cin>>t;
	// 			catalog.DropTable(t);
	// 			catalog.Save();
	// 			break;
	// 		}
	// 		case 4: return 0;
	// 	}
	// }
	return 0;
}
