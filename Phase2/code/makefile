CC = g++ -std=c++11 -g -O0 -Wno-deprecated

tag = -i

ifdef linux
	tag = -n
endif


main.out:	QueryParser.o QueryLexer.o Schema.o Record.o File.o DBFile.o Comparison.o Function.o RelOp.o Catalog.o DataTypeClass.o QueryOptimizer.o QueryCompiler.o main.o
	$(CC) -o main.out main.o QueryParser.o QueryLexer.o Schema.o Record.o File.o DBFile.o Comparison.o Function.o RelOp.o Catalog.o DataTypeClass.o QueryOptimizer.o QueryCompiler.o -ll -lsqlite3
	
main.o:	main.cc
	$(CC) -c main.cc
	
Schema.o: Schema.cc	
	$(CC) -c Schema.cc

Record.o: Schema.cc Record.cc
	$(CC) -c Record.cc

File.o: Schema.cc Record.cc File.cc
	$(CC) -c File.cc

DBFile.o: Schema.cc Record.cc File.cc DBFile.cc
	$(CC) -c DBFile.cc

Comparison.o: Schema.cc Record.cc Comparison.cc
	$(CC) -c Comparison.cc

Function.o: Schema.cc Record.cc Function.cc
	$(CC) -c Function.cc

RelOp.o: Schema.cc Record.cc Comparison.cc RelOp.cc	
	$(CC) -c RelOp.cc
	
QueryOptimizer.o: Schema.cc Record.cc Comparison.cc RelOp.cc QueryOptimizer.cc	
	$(CC) -c QueryOptimizer.cc

QueryCompiler.o: Schema.cc Record.cc Comparison.cc RelOp.cc QueryOptimizer.cc QueryCompiler.cc	
	$(CC) -c QueryCompiler.cc

Catalog.o: Schema.cc Catalog.cc	
	$(CC) -c Catalog.cc

DataTypeClass.o: DataTypeClass.cc
	$(CC) -c DataTypeClass.cc

QueryParser.o: QueryParser.y
	yacc --defines=QueryParser.h -o QueryParser.c QueryParser.y
	sed $(tag) -e "s/  __attribute__ ((__unused__))$$/# ifndef __cplusplus\n  __attribute__ ((__unused__));\n# endif/" QueryParser.c
	g++ -c QueryParser.c

QueryLexer.o: QueryLexer.l
	lex -o QueryLexer.c QueryLexer.l
	gcc -c QueryLexer.c


clean: 
	rm -f *.o
	rm -f *.out
	rm -f QueryLexer.c
	rm -f QueryParser.c
	rm -f QueryParser.c-e
	rm -f QueryParser.h

