#ifndef _QUERY_OPTIMIZER_H
#define _QUERY_OPTIMIZER_H

#include "Schema.h"
#include "Catalog.h"
#include "ParseTree.h"
#include "RelOp.h"

#include <string>
#include <vector>
#include <unordered_map>

using namespace std;

// data structure used by the optimizer to compute join ordering
struct OptimizationTree {
	// list of tables joined up to this node
	vector<string> tables;
	// number of tuples in each of the tables (after selection predicates)
	vector<int> tuples;
	// number of tuples at this node
	int noTuples;

	// connections to children and parent
	OptimizationTree* parent;
	OptimizationTree* leftChild;
	OptimizationTree* rightChild;
};

struct tableTuple {
	unsigned long long size;
	unsigned long long cost;
	OptimizationTree *order;
	Schema schema;

};

class QueryOptimizer {
private:
	Catalog* catalog;

	TableList *tables;
	AndList *predicate;
	OptimizationTree *root;

	unordered_map<string, tableTuple> t_map;
	unsigned long long permuatationSize;
	
public:
	QueryOptimizer(Catalog& _catalog);
	virtual ~QueryOptimizer();

	void Optimize(TableList* _tables, AndList* _predicate, OptimizationTree* _root);
	
	unsigned long long pushDownSelection(string &tableName);
	void initializeTablePair();
	void partitionTables(int _numTables, vector<string> &_tableNames);
	string getTableKey(vector<string> _tableNames);
	void generatePermutation(vector<vector<int>> &_permutations, vector<int> &_tempVector, int size);
	unsigned long long factorial(int _numTables);
	
	unsigned long long Estimate_Join_Cardinality(Schema &sch1, Schema &sch2, unsigned long long tblsize);
};

#endif // _QUERY_OPTIMIZER_H
