#ifndef _SWAP_H_
#define _SWAP_H_

/**
	This header contains macro definitions to streamline the swapping of data.
	The main macro defined is SWAP(a,b) that swaps the content of a and b as
	long as they are of the same type and they support the = operator.

	Note that the type of a and b is not known and it is inferred using the
	typeof() operator.
*/

// typeof() is deprecated as of c++11 so we switched to decltype()
#define SWAP(a,b) {decltype(a) tmp=a; a=b; b=tmp;}
// TODO: Get the below command working on make
//#define SWAP(a,b) {decltype(a) a^=b; b^=a; a^=b;}

/* macro to swap content of STL-like containers/maps */
#define STL_SWAP(a,b) a.swap(b);

#endif //_SWAP_H_
