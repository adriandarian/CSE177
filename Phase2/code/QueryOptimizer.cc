#include <string>
#include <vector>
#include <iostream>

#include "Schema.h"
#include "Comparison.h"
#include "QueryOptimizer.h"
#include <map>

using namespace std;


QueryOptimizer::QueryOptimizer(Catalog& _catalog) : catalog(&_catalog) {
}

QueryOptimizer::~QueryOptimizer() {
}

void QueryOptimizer::Optimize(TableList* _tables, AndList* _predicate, OptimizationTree* _root) {
	// compute the optimal join order
	//int nTbl = 0;
	//int noTuples = 0;
	TableList* tablesList = _tables;
	vector<string> tables;
	OptimizationTree* temp = new OptimizationTree;	

	for (TableList* node = _tables; node != NULL; node = node->next) {	
		//nTbl += 1;
		tables.push_back(string(node->tableName));
	}

	temp = BuildTree(_tables, _root, tables);
	*_root = *temp;
}

OptimizationTree* QueryOptimizer::BuildTree(TableList* _tables, OptimizationTree* _root, vector<string> tabs) {
	vector<string> tables; 
	tables = tabs;
	OptimizationTree* root = _root;
	TableList* tablesList = _tables;
	

	if(tables.size() == 0) {
		root = NULL;
		return root;
	}
	else {
		//Initialize
		OptimizationTree* node = new OptimizationTree;
		OptimizationTree* left = new OptimizationTree;
		OptimizationTree* right = new OptimizationTree;
		//Push back for children
		node->tables.push_back(tables[0]);
		//TODO: Better way to do this?
		if(tables.size() == 1) {
			node->leftChild = NULL;
			node->rightChild = NULL;
			return node;
		}
		else {
			node->tables.push_back(tables[1]);
			left->tables.push_back(tables[0]);
			right->tables.push_back(tables[1]);

			node->leftChild = left;
			left->leftChild = NULL;
			right->leftChild = NULL;

			node->rightChild = right;
			left->rightChild = NULL;
			right->rightChild = NULL;
			node->parent = NULL;

			left->parent = node;
			right->parent = node;

			if(tables.size() == 2) {
				return node;
			}
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
		}
		return node;
	}
}