Join ordering algorithm
-----------------------
Input
+++++
1. table names and total number of tables NT
2. selection predicates
3. join predicates

Data structures
+++++++++++++++
1. Map : map <table_name> -> tuple (size, cost, order)
-- table_name : a name that puts all the table names together, when there are more
-- size : the cardinality, i.e., number of tuples, in table(s)
-- cost : the sum of intermediate number of tuples in the tree (query optimization cost)
-- order : join order with groupings of tables explicit

Pre-processing
++++++++++++++
1. fill the map for every individual table name
-- Map<(T)> = (size from catalog, 0, (T))
2. push-down selections: update the size for each table based on the estimation of
   number of tuples following the application of selection predicates
-- Map<(T)> = (size after push-down selection, 0, (T))
3. fill the map for every pair of two tables (T1,T2)
-- sort ascending T1, T2 (no need to have entries in Map for (T1,T2) and (T2,T1)
   because of commutativity)
-- estimate cardinality of join using distinct values from Catalog and join predicates; if you do not have a join predicate between two tables, the cost is infinity
-- Map<(T1,T2)> = (Map<(T1)>.size*Map<(T2)>.size/selectivity, 0, (T1,T2))


Full enumeration algorithm with dynamic programming
+++++++++++++++++++++++++++++++++++++++++++++++++++
- consider all the permutations at the top and build all possible tree shapes for
  every permutation
- use and update Map as the algorithm progresses

Partition (N, array[tables])
- build all possible tree shapes by repeated partitioning
1. if (Map<sort-asc(T1,T2,...,Tn)> is not empty) return
2. for i = 1 to N! do
		 Let (T1',T2',...,Tn') be the ith permutation of N tables. You can find
		 algorithms on how to generate this permutation on Wikipedia: Permutation.
3.   for j = 1 to N-1 do
			 left = (T1',...,Tj')
			 if (Map<sort-asc(left)> is empty) Partition (j, left)
			 right = (T(j+1)',...,Tn')
			 if (Map<sort-asc(right)> is empty) Partition (N-j, right)

4.		 // at this point we are guaranteed to have entries in the Map on left and right
			 // compute the cost for this tree and store, if minimum; otherwise, discard
			 cost = Map<sort-asc(left)>.cost + Map<sort-asc(right)>.cost
			 if (j!=1) cost += Map<sort-asc(left)>.size
			 if (j!=N-1) cost += Map<sort-asc(right)>.size
			 if (cost < min_cost)
				 // estimate cardinality of join using distinct values from Catalog and join predicates
			   size = Map<(left)>.size*Map<(right)>.size/selectivity
			 	 order = (Map<sort-asc(left)>.order,Map<sort-asc(right)>.order)
				 min_cost = cost
5. Map<sort-asc(T1,T2,...,Tn)> = (size,min_cost,order)


Graph traversal algorithm
+++++++++++++++++++++++++
1. build a graph in which the tables are the nodes; an edge exists between two nodes only if there is a join predicate between the two corresponding tables, i.e., the number of edges is equal with the number of join predicates (the graph should form a single connected component)
2. to get a valid join ordering, you have to do a traversal of the graph; the best join order corresponds to the traversal with the smallest cost
3. for example, you can get a graph traversal by building the minimum spanning tree of the graph with the cost of each edge estimated as the join size of the two tables
4. a graph traversal A-B-C-D maps into the same join ordering A-B-C-D
