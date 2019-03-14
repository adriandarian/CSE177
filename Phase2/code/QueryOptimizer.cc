#include <string>
#include <vector>
#include <iostream>

#include "Schema.h"
#include "Comparison.h"
#include "QueryOptimizer.h"
#include <map>

using namespace std;

struct tuples {
	Schema schema;
	unsigned long long int size;
	unsigned long long int cost;
	string order;
};

	map <string, tuples> aMap;
QueryOptimizer::QueryOptimizer(Catalog& _catalog) : catalog(&_catalog) {
}

QueryOptimizer::~QueryOptimizer() {
}

void QueryOptimizer::Optimize(TableList* _tables, AndList* _predicate, OptimizationTree* _root) {
	// compute the optimal join order
	int nTbl = 0;
	int noTuples = 0;
	TableList* tablesList = _tables;
	vector<string> tables;

	for (TableList* node = _tables; node != NULL; node = node->next) {	
		nTbl += 1;
		tables.push_back(string(node->tableName));
	}
	BuildTree(_tables, _root, tables);
}

void QueryOptimizer::BuildTree(TableList* _tables, OptimizationTree* _root, vector<string> tabs) {
	vector<string> tables; 
	tables = tabs;
	OptimizationTree* root = _root;
	TableList* tablesList = _tables;

	if(tabs.size() == 0) {
		_root = NULL;
	}
	else {
		//Initialize
		OptimizationTree* node = new OptimizationTree;
		OptimizationTree* left = new OptimizationTree;
		OptimizationTree* right = new OptimizationTree;
		//Push back for children
		node->tables.push_back(tables[0]);
		//TODO: Better way to do this?
		if(tables.size == 1) {
			node->leftChild = NULL;
			node->rightChild = NULL;
			*_root = *node;
		}
		else {
			node->tables.push_back(tables[1]);
			left->tables.push_back(tables[0]);
			right->tables.push_back(tables[1]);

			node->leftChild = left;
			node->rightChild = right;
			node->parent = NULL;
			left->leftChild = NULL;
			left->rightChild = NULL;
			right->leftChild = NULL;
			right->rightChild = NULL;
			left->parent = node;
			right->parent = node;

			OptimizationTree* temp = new OptimizationTree;
			//At 2 because we did 0 and 1 already for edge cases
			for(int i = 2; i < tables.size(); ++i) {
				OptimizationTree* tempN = new OptimizationTree;
				OptimizationTree* tempL = new OptimizationTree;
				OptimizationTree* tempR = new OptimizationTree;
				tempL = root;
				tempL->parent = tempN;
				tempR->tables.push_back(tables[i]);
				tempR->leftChild = NULL;
				tempR->rightChild = NULL;
				tempR->parent = node;
				for(int j = 0; j < i; ++j) {
					tempN->tables.push_back(tables[i]);
				}
				tempN->leftChild = tempL;
				tempN->rightChild = tempR;
				tempN->parent = NULL;
				node = tempN;
				
			}
			
			*_root = *node;
		}
	}
}