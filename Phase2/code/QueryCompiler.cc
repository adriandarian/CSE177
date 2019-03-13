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
	
	for (int i = 0; i < nTbl; i++) cout << *forest[i] << endl;



	// push-down selections: create a SELECT operator wherever necessary
	for (int i = 0; i < nTbl; i++) {
		Record literal;
		CNF cnf;
		int ret = cnf.ExtractCNF (*_predicate, forestSchema[i], literal);
		if (0 != ret) exit(1);

		RelationalOp* op = new Select(forestSchema[i], cnf, literal, forest[i]);
		forest[i] = op;
	}

	for (int i = 0; i < nTbl; i++) cout << *forest[i] << endl;


	// call the optimizer to compute the join order
	OptimizationTree* root;
	optimizer->Optimize(_tables, _predicate, root);

	// create join operators based on the optimal order computed by the optimizer

	// create the remaining operators based on the query

	// connect everything in the query execution tree and return

	// free the memory occupied by the parse tree since it is not necessary anymore
}
