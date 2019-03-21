#ifndef _QUERY_OPTIMIZER_H
#define _QUERY_OPTIMIZER_H

#include "Schema.h"
#include "Catalog.h"
#include "ParseTree.h"
#include "RelOp.h"

#include <string>
#include <vector>

using namespace std;


// data structure used by the optimizer to compute join ordering
struct OptimizationTree {

	int id;

	// list of tables joined up to this node
	vector<string> tables;
	// number of tuples in each of the tables (after selection predicates)
	vector<int> tuples;
	// number of tuples at this node
	int noTuples;
	bool alreadyJoined;
	// connections to children and parent
	OptimizationTree* parent;
	OptimizationTree* leftChild;
	OptimizationTree* rightChild;
};

struct joins {
	string leftTable;
	string leftAttribute;

	string rightTable;
	string rightAttribute;


};

class QueryOptimizer {
private:
	Catalog* catalog;

public:
	QueryOptimizer(Catalog& _catalog);
	
	string getPredicateTable(string _predicateValue);
	vector<string> convertTableListToVector(TableList *_tables);

	void printNode(OptimizationTree* node);
	void printAllNodes(vector<OptimizationTree*> myNodes);
	void printJoin(joins*tree);
	void printAllJoins(vector<joins*> join);

	void applyPushDownSelections(vector<OptimizationTree*>initialNodes, AndList*_predicate);
	void getAllJoins(vector<joins*>&allJoins, AndList*_predicate);
	void estimateMinCostOfJoins(vector<OptimizationTree*> initialNodes, vector<joins*>allJoins, long &minCost, int &index);

	virtual ~QueryOptimizer();

	void Optimize(TableList* _tables, AndList* _predicate, OptimizationTree* _root);
};

#endif // _QUERY_OPTIMIZER_H
