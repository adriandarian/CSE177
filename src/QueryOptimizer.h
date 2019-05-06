#ifndef _QUERY_OPTIMIZER_H
#define _QUERY_OPTIMIZER_H

#include "Schema.h"
#include "Catalog.h"
#include "ParseTree.h"
#include "RelOp.h"

#include <string>
#include <vector>
#include <map>
using namespace std;

//Following code on optimiztion.alg to come up with data structures

// data structure used by the optimizer to compute join ordering
// struct OptimizationTree {
// 	// list of tables joined up to this node
// 	vector<string> tables;
// 	// number of tuples in each of the tables (after selection predicates)
// 	vector<int> tuples;
// 	// number of tuples at this node
// 	int noTuples;

// 	// connections to children and parent
// 	OptimizationTree* parent;
// 	OptimizationTree* leftChild;
// 	OptimizationTree* rightChild;
// };
struct OptimizationTree
{
	// list of tables joined up to this node
	vector<string> tables;
	//number of tuples
	unsigned long int size;

	// connections to children and parent
	OptimizationTree *parent;
	OptimizationTree *leftChild;
	OptimizationTree *rightChild;
	Schema schema;

	~OptimizationTree()
	{
		//cout << "delete OptimizationTree node" << endl;
	}
};
// struct tuple{
// 	int size;
// 	int cost;
// 	OptimizationTree* order;
// };

class QueryOptimizer
{
private:
	Catalog *catalog;
	//map <string,tuple> omap;
	vector<OptimizationTree *> tvec;
	vector<OptimizationTree *> cvec;
	OptimizationTree *root;
	vector<OptimizationTree *> everything;

public:
	QueryOptimizer(Catalog &_catalog);
	virtual ~QueryOptimizer();

	void Optimize(TableList *_tables, AndList *_predicate, OptimizationTree *_root);

	unsigned long int calcPushDown(AndList *_predicate, Schema schema, unsigned long int numT);
	unsigned long int EstJoin(AndList *_predicate, Schema schema1, Schema schema2, unsigned long int num);
	void print();
	void postOrder(OptimizationTree *node);
};

#endif // _QUERY_OPTIMIZER_H
