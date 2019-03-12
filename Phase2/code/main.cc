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
	// this is the catalog
	string dbFile = "catalog.sqlite";
	Catalog catalog(dbFile);

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
	}
	else {
		cout << "Error: Query is not correct!" << endl;
		parse = -1;
	}

	yylex_destroy();

	if (parse != 0) return -1;

	// at this point we have the parse tree in the ParseTree data structures
	// we are ready to invoke the query compiler with the given query
	// the result is the execution tree built from the parse tree and optimized
	QueryExecutionTree queryTree;
	compiler.Compile(tables, attsToSelect, finalFunction, predicate,
		groupingAtts, distinctAtts, queryTree);

	cout << queryTree << endl;

/*
	vector<string> attributes;
	vector<string> attributeTypes;
	vector<unsigned int> distincts;

	attributes.push_back("l_orderkey");
	attributeTypes.push_back("INTEGER");
	distincts.push_back(1500000);

	attributes.push_back("l_partkey");
	attributeTypes.push_back("INTEGER");
	distincts.push_back(200000);

	attributes.push_back("l_suppkey");
	attributeTypes.push_back("INTEGER");
	distincts.push_back(10000);

	attributes.push_back("l_linenumber");
	attributeTypes.push_back("INTEGER");
	distincts.push_back(7);

	attributes.push_back("l_quantity");
	attributeTypes.push_back("FLOAT");
	distincts.push_back(50);

	attributes.push_back("l_extendedprice");
	attributeTypes.push_back("FLOAT");
	distincts.push_back(933900);

	attributes.push_back("l_discount");
	attributeTypes.push_back("FLOAT");
	distincts.push_back(11);

	attributes.push_back("l_tax");
	attributeTypes.push_back("FLOAT");
	distincts.push_back(9);

	attributes.push_back("l_returnflag");
	attributeTypes.push_back("STRING");
	distincts.push_back(3);

	attributes.push_back("l_linestatus");
	attributeTypes.push_back("STRING");
	distincts.push_back(2);

	attributes.push_back("l_shipdate");
	attributeTypes.push_back("STRING");
	distincts.push_back(2526);

	attributes.push_back("l_commitdate");
	attributeTypes.push_back("STRING");
	distincts.push_back(2466);

	attributes.push_back("l_receiptdate");
	attributeTypes.push_back("STRING");
	distincts.push_back(2554);

	attributes.push_back("l_shipinstruct");
	attributeTypes.push_back("STRING");
	distincts.push_back(4);

	attributes.push_back("l_shipmode");
	attributeTypes.push_back("STRING");
	distincts.push_back(7);

	attributes.push_back("l_comment");
	attributeTypes.push_back("STRING");
	distincts.push_back(4580667);

	Schema lineitem(attributes, attributeTypes, distincts);
	cout << lineitem << endl;

	Record literal;
	CNF cPredicate;
	cPredicate.ExtractCNF(*predicate, lineitem, literal);
	cout << cPredicate << endl;

	Schema litSchema(lineitem);
	vector<int> attsToKeep;
	litSchema.Project(attsToKeep);
	cout << litSchema << endl;
	cout << lineitem << endl;

	literal.print(cout, litSchema); cout << endl;

	Record rr = literal;

	EfficientMap<Record, KeyInt> emRecord;
	multimap<Record, int> mmRecord;

	mmRecord.insert(pair<Record, int>(literal, 10));

	KeyInt ki(10);
	emRecord.Insert(rr, ki);
*/

	return 0;
}
