#ifndef __DBLAB_ENGINE_H__
#define __DBLAB_ENGINE_H__

// Domain specific libraries should be put here and not in CCodeGenerator of pardis
#include <glib.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/time.h>
#include <sys/mman.h>
#include <sys/types.h>
#include <sys/stat.h>
#include "dblab_clib.h"

#define AGG_OP_TAG 1
#define PRINT_OP_TAG 2 
#define SCAN_OP_TAG 3
#define SELECT_OP_TAG 4
#define SORT_OP_TAG 5
#define MAP_OP_TAG 6
#define HASHJOIN_OP_TAG 7
#define MERGEJOIN_OP_TAG 8
#define WINDOW_OP_TAG 9
#define LEFTSEMIHASHJOIN_OP_TAG 10

struct operator_t {
  numeric_int_t tag;
  void* parent;
};

typedef void* record_t;

typedef record_t (*lambda_t)();

typedef double (*lambda_double_t)();

typedef numeric_int_t (*lambda_int_t)();

typedef boolean_t (*lambda_boolean_t)();

struct scanop_t {
  numeric_int_t tag;
  numeric_int_t table_size;
  numeric_int_t counter;
  numeric_int_t record_size;
  record_t table;
};

struct printop_t {
  numeric_int_t tag;
  struct operator_t* parent;
  lambda_t printFunc;
  numeric_int_t limit;
};

struct aggop_t {
  numeric_int_t tag;
  struct operator_t* parent;
  numeric_int_t numAggs;
  GHashTable* hm;
  GList* hm_keys;
  numeric_int_t hm_iter_counter;
  lambda_t grp;
  lambda_t agger;
  GList* aggFuncsOutput;
};

struct selectop_t {
  numeric_int_t tag;
  struct operator_t* parent;
  lambda_boolean_t selectPred;
};

struct hashjoin_t {
  numeric_int_t tag;
  struct operator_t* leftParent;
  struct operator_t* rightParent;
  GHashTable* hm;
  lambda_boolean_t joinCond;
  lambda_t leftHash;
  lambda_t rightHash;
  lambda_t concatenator;
};

struct mergejoin_t {
  numeric_int_t tag;
  struct operator_t* leftParent;
  struct operator_t* rightParent;
  lambda_int_t joinCond;
  record_t* leftRelation;
  numeric_int_t leftIndex;
  numeric_int_t leftSize;
  lambda_t concatenator;
};

struct sortop_t {
  numeric_int_t tag;
  struct operator_t* parent;
  GTree* sortedTree;
};

struct mapop_t {
  numeric_int_t tag;
  struct operator_t* parent;
  numeric_int_t mapNums;
  GList** mapFuncs;
};

struct windowop_t {
  numeric_int_t tag;
  struct operator_t* parent;
  lambda_t grp;
  lambda_t wndFunction;
  lambda_t wndFactory;
  GHashTable* hm;
};

struct leftsemihashjoin_t {
  numeric_int_t tag;
  struct operator_t* leftParent;
  struct operator_t* rightParent;
  GHashTable* hm;
  lambda_t joinCond;
  lambda_t leftHash;
  lambda_t rightHash;
};


struct agg_rec_t {
  record_t key;
  double* aggs;
};

void operator_open(struct operator_t* op);
record_t operator_next(struct operator_t* op);

void selectop_open(struct operator_t* op) {
  struct operator_t* parent = op->parent;
  operator_open(parent);
}

record_t selectop_next(struct operator_t* op) {
  struct operator_t* parent = op->parent;
  struct selectop_t* selectop = (struct selectop_t*)op;
  while(true) {
    record_t t = operator_next(parent);
    if(t == NULL) {
      return NULL;
    } else if (selectop->selectPred(t)){
      return t;
    } else {
      continue;
    }
  }
}

void mapop_open(struct operator_t* op) {
  struct operator_t* parent = op->parent;
  operator_open(parent);
}

record_t mapop_next(struct operator_t* op) {
  struct operator_t* parent = op->parent;
  struct mapop_t* mapop = (struct mapop_t*)op;
  record_t t = operator_next(parent);
  if(t == NULL) {
    return NULL;
  } else {
    for(int i=0; i<mapop->mapNums; i++) {
      lambda_t func = g_list_nth_data(mapop->mapFuncs, i);
      func(t);
    }
    return t;
  }
}

void sortop_open(struct operator_t* op) {
  struct sortop_t* sop = (struct sortop_t*)op;
  operator_open(sop->parent);
  while(true) {
    record_t t = operator_next(sop->parent);
    if(t == NULL) {
      break;
    } else {
      g_tree_insert(sop->sortedTree, t, t);
    }
  }
}

numeric_int_t treeHead(void* x6081, void* x6082, void* x6083) {
  pointer_assign((record_t*)x6083, x6082);
  return 1; 
}

record_t sortop_next(struct operator_t* op) {
  struct sortop_t* sop = (struct sortop_t*)op;
  int size = g_tree_nnodes(sop->sortedTree);
  if(size == 0) {
    return NULL;
  } else {
    record_t result_tuple = NULL;
    g_tree_foreach(sop->sortedTree, treeHead, &result_tuple);
    g_tree_remove(sop->sortedTree, result_tuple);
    return result_tuple;
  }
}

void hashjoinop_open(struct operator_t* op) {
  struct hashjoin_t* hjop = (struct hashjoin_t*)op;
  operator_open(hjop->leftParent);
  operator_open(hjop->rightParent);
  hjop->hm = g_hash_table_new(g_direct_hash, g_direct_equal);
  while (true) {
    record_t t = operator_next(hjop->leftParent);
    if(t == NULL) {
      break;
    } else {
      record_t key = (record_t){hjop->leftHash(t)};
      GList** values = (GList**){g_hash_table_lookup(hjop->hm, key)};
      if(values == NULL) {
        GList** tmpList = malloc(8);
        GList* tmpList1 = NULL;
        pointer_assign(tmpList, tmpList1);
        values = tmpList;
      }
      GList* valuesList = *(values);
      valuesList = g_list_prepend(valuesList, t);
      pointer_assign(values, valuesList);
      g_hash_table_insert(hjop->hm, key, (void*){values});
    }
  }
}

// only supports 1 to N cases
record_t hashjoinop_next(struct operator_t* op) {
  struct hashjoin_t* hjop = (struct hashjoin_t*)op;
  // printf("size of table %d\n", g_hash_table_size(hjop->hm));
  while (true) {
    record_t t = operator_next(hjop->rightParent);
    if(t == NULL) {
      break;
    } else {
      record_t key = (record_t){hjop->rightHash(t)};
      GList** values = (GList**){g_hash_table_lookup(hjop->hm, key)};
      if(values != NULL) {
        GList* list = *values;
        int n = g_list_length(list);
        boolean_t found = false;
        record_t result_value = NULL;
        for(int i = 0; i<n; i++) {
          record_t leftElem = g_list_nth_data(list, i);
          if(hjop->joinCond(leftElem, t)) {
            // if(found) {
            //   printf("WARNING! ** The relationship is not 1-N in HashJoin! **\n");
            // }
            result_value = hjop->concatenator(leftElem, t); 
            found = true;
          }  
        }
        if(found) {
          return result_value;
        }
      }
    }
  }
  return NULL;
}

void leftsemihashjoinop_open(struct operator_t* op) {
  struct leftsemihashjoin_t* hjop = (struct leftsemihashjoin_t*)op;
  operator_open(hjop->leftParent);
  operator_open(hjop->rightParent);
  hjop->hm = g_hash_table_new(g_direct_hash, g_direct_equal);
  while (true) {
    record_t t = operator_next(hjop->rightParent);
    if(t == NULL) {
      break;
    } else {
      record_t key = (record_t){hjop->rightHash(t)};
      GList** values = (GList**){g_hash_table_lookup(hjop->hm, key)};
      if(values == NULL) {
        GList** tmpList = malloc(8);
        GList* tmpList1 = NULL;
        pointer_assign(tmpList, tmpList1);
        values = tmpList;
      }
      GList* valuesList = *(values);
      valuesList = g_list_prepend(valuesList, t);
      pointer_assign(values, valuesList);
      g_hash_table_insert(hjop->hm, key, (void*){values});
    }
  }
}

// only supports 1 to N cases
record_t leftsemihashjoinop_next(struct operator_t* op) {
  struct leftsemihashjoin_t* hjop = (struct leftsemihashjoin_t*)op;
  // printf("size of table %d\n", g_hash_table_size(hjop->hm));
  while (true) {
    record_t t = operator_next(hjop->leftParent);
    if(t == NULL) {
      break;
    } else {
      record_t key = (record_t){hjop->leftHash(t)};
      GList** values = (GList**){g_hash_table_lookup(hjop->hm, key)};
      if(values != NULL) {
        GList* list = *values;
        int n = g_list_length(list);
        boolean_t found = false;
        for(int i = 0; i<n; i++) {
          record_t rightElem = g_list_nth_data(list, i);
          if(hjop->joinCond(t, rightElem)) {
            found = true;
            break;
          }  
        }
        if(found) {
          return t;
        }
      }
    }
  }
  return NULL;
}

// hack! should be in the mergejoin struct itself!
record_t mjop_leftElem;
record_t mjop_rightElem;

void mergejoinop_open(struct operator_t* op) {
  struct mergejoin_t* mjop = (struct mergejoin_t*)op;
  operator_open(mjop->leftParent);
  operator_open(mjop->rightParent);
  mjop_leftElem = operator_next(mjop->leftParent);
  mjop_rightElem = operator_next(mjop->rightParent);
}

// only supports 1 to N cases
record_t mergejoinop_next(struct operator_t* op) {
  struct mergejoin_t* mjop = (struct mergejoin_t*)op;
  while (true) {
    if(mjop_leftElem == NULL || mjop_rightElem == NULL)
      break;
    int condRes = mjop->joinCond(mjop_leftElem, mjop_rightElem);
    record_t res = NULL;
    if(condRes == 0) {
      res = mjop->concatenator(mjop_leftElem, mjop_rightElem);
      // mjop_leftElem = operator_next(mjop->leftParent);
      mjop_rightElem = operator_next(mjop->rightParent);
    } else if (condRes < 0) {
      mjop_leftElem = operator_next(mjop->leftParent);
    } else {
      mjop_rightElem = operator_next(mjop->rightParent);
    }
    if(res != NULL) {
      return res;
    }
  }
  return NULL;
}

void aggop_open(struct operator_t* op) {
  struct operator_t* parent = op->parent;
  operator_open(parent);
  struct aggop_t* aggop = (struct aggop_t*)op;
  while (true) {
    record_t t = operator_next(parent);
    if(t == NULL) {
      break;
    } else {
      // printf("agging %d\n", aggop->numAggs);
      record_t key = aggop->grp(t);
      struct agg_rec_t* value = g_hash_table_lookup(aggop->hm, key);
      if(value==(NULL)) {
        struct agg_rec_t* new_value = aggop->agger(key);
        g_hash_table_insert(aggop->hm, key, new_value);
        value = new_value;
      }
      for(int i = 0; i<aggop->numAggs; i++) {
        lambda_double_t func = g_list_nth_data(aggop->aggFuncsOutput, i);
        value->aggs[i] = func(t, value->aggs[i]);
        // printf("agg[%d]=%f\n", i, value->aggs[i]);
      }
    }
  }
  // printf("Size of table = %d\n", g_hash_table_size(aggop->hm));
  g_hash_table_get_keys(aggop->hm);
  aggop->hm_keys = g_hash_table_get_keys(aggop->hm);
}

record_t aggop_next(struct operator_t* op) {
  struct aggop_t* aggop = (struct aggop_t*)op;
  int size = g_hash_table_size(aggop->hm);
  // printf("%d size of table\n", size);
  if(aggop->hm_iter_counter < size) {
    record_t key = g_list_nth_data(aggop->hm_keys, aggop->hm_iter_counter);
    record_t elem = g_hash_table_lookup(aggop->hm, key);
    aggop->hm_iter_counter++;
    return elem;
  } else {
    return NULL;
  }
}

// TODO needs to be moved to the windowop struct
GList* windowop_hm_keys;
int windowop_hm_iter_counter;

void windowop_open(struct operator_t* op) {
  struct operator_t* parent = op->parent;
  operator_open(parent);
  struct windowop_t* windowop = (struct windowop_t*)op;
  windowop->hm = g_hash_table_new(g_direct_hash, g_direct_equal);
  while (true) {
    record_t t = operator_next(parent);
    if(t == NULL) {
      break;
    } else {
      record_t key = (record_t){windowop->grp(t)};
      GList** values = (GList**){g_hash_table_lookup(windowop->hm, key)};
      if(values == NULL) {
        GList** tmpList = malloc(8);
        GList* tmpList1 = NULL;
        pointer_assign(tmpList, tmpList1);
        values = tmpList;
      }
      GList* valuesList = *(values);
      valuesList = g_list_prepend(valuesList, t);
      pointer_assign(values, valuesList);
      g_hash_table_insert(windowop->hm, key, (void*){values});
    }
  }
  // printf("Size of table = %d\n", g_hash_table_size(windowop->hm));
  windowop_hm_keys = g_hash_table_get_keys(windowop->hm);
}

record_t windowop_next(struct operator_t* op) {
  struct windowop_t* windowop = (struct windowop_t*)op;
  int size = g_hash_table_size(windowop->hm);
  // printf("%d size of table\n", size);
  if(windowop_hm_iter_counter < size) {
    record_t key = g_list_nth_data(windowop_hm_keys, windowop_hm_iter_counter);
    record_t elem = g_hash_table_lookup(windowop->hm, key);
    windowop_hm_iter_counter++;
    record_t result = windowop->wndFactory(key, windowop->wndFunction(elem));
    return result;
  } else {
    return NULL;
  }
}

void scanop_open(struct operator_t* op) {
  struct scanop_t* scanop = (struct scanop_t*)op;
  // printf("scan op relation of size %d\n", scanop->table_size);
}

record_t scanop_next(struct operator_t* op) {
  struct scanop_t* scanop = (struct scanop_t*)op;
  // printf("scan op next[%lld] of %lld\n", scanop->counter, scanop->table_size);
  if (scanop->counter < scanop->table_size) {
  // if (scanop->counter < 10) {
    record_t v = scanop->table + scanop->counter * scanop->record_size;
    scanop->counter += 1;
    return v;
  } else {
    return NULL;
  }
}

void printop_open(struct operator_t* op) {
  struct operator_t* parent = op->parent;
  struct printop_t* pop = (struct printop_t*) op;
  // printf("print op open with limit %lld\n", pop->limit);
  operator_open(parent);
}

record_t printop_next(struct operator_t* op) {
  struct operator_t* parent = op->parent;
  struct printop_t* pop = (struct printop_t*) op;
  int numRows = 0;
  boolean_t exit = false;
  while (!exit) {
    record_t t = operator_next(parent);
    if((pop->limit != -1 && numRows >= pop->limit) || t == NULL) {
      exit = true;
    } else {
      // printf("printing %d: f=%d\n", numRows, pop->printFunc);
      pop->printFunc(t);
      numRows += 1;
    }
  }
  printf("(%d rows)\n", numRows);
  return 0;
}

void printop_run(void* raw_op) {
  struct operator_t* op = raw_op;
  printop_open(op);
  printop_next(op);
}

// TODO rest of operators
void operator_open(struct operator_t* op) {  
  // printf("Open with tag %d!\n", op->tag);
  switch(op->tag) {
    case AGG_OP_TAG: aggop_open(op); break;
    case PRINT_OP_TAG: printop_open(op); break;
    case SCAN_OP_TAG: scanop_open(op); break;
    case SELECT_OP_TAG: selectop_open(op); break;
    case SORT_OP_TAG: sortop_open(op); break;
    case MAP_OP_TAG: mapop_open(op); break;
    case HASHJOIN_OP_TAG: hashjoinop_open(op); break;
    case MERGEJOIN_OP_TAG: mergejoinop_open(op); break;
    case WINDOW_OP_TAG: windowop_open(op); break;
    case LEFTSEMIHASHJOIN_OP_TAG: leftsemihashjoinop_open(op); break;
    default: printf("Default Open with tag %d!\n", op->tag);
  }
}

// TODO rest of operators
void* operator_next(struct operator_t* op) {
  // printf("Next with tag %d!\n", op->tag);
  switch(op->tag) {
    case AGG_OP_TAG: return aggop_next(op); 
    case PRINT_OP_TAG: return printop_next(op);
    case SCAN_OP_TAG: return scanop_next(op);
    case SELECT_OP_TAG: return selectop_next(op);
    case SORT_OP_TAG: return sortop_next(op);
    case MAP_OP_TAG: return mapop_next(op);
    case HASHJOIN_OP_TAG: return hashjoinop_next(op);
    case MERGEJOIN_OP_TAG: return mergejoinop_next(op);
    case WINDOW_OP_TAG: return windowop_next(op);
    case LEFTSEMIHASHJOIN_OP_TAG: return leftsemihashjoinop_next(op);
    default: printf("Default Next with tag %d!\n", op->tag); return 0;
  }
}

#endif
