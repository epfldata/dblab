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

struct operator_t {
  numeric_int_t tag;
  void* parent;
};

typedef void* record_t;

typedef record_t (*lambda_t)();

typedef double (*lambda_double_t)();

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

struct agg_rec_t {
  record_t key;
  double* aggs;
};

void operator_open(struct operator_t* op);
record_t operator_next(struct operator_t* op);

GList* Seq(void* e1) {
	GList* result = NULL;
	result = g_list_append (result, e1);
	return result;
}

// TODO
void selectop_open(struct operator_t* op) {
  struct operator_t* parent = op->parent;
  operator_open(parent);
}

// TODO
record_t selectop_next(struct operator_t* op) {
  struct operator_t* parent = op->parent;
  return operator_next(parent);
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
  switch(op->tag) {
    case AGG_OP_TAG: aggop_open(op); break;
    case PRINT_OP_TAG: printop_open(op); break;
    case SCAN_OP_TAG: scanop_open(op); break;
    case SELECT_OP_TAG: selectop_open(op); break;
    default: printf("Default Open with tag %d!\n", op->tag);
  }
}

// TODO rest of operators
void* operator_next(struct operator_t* op) {
  switch(op->tag) {
    case AGG_OP_TAG: return aggop_next(op); 
    case PRINT_OP_TAG: return printop_next(op);
    case SCAN_OP_TAG: return scanop_next(op);
    case SELECT_OP_TAG: return selectop_next(op);
    default: printf("Default Next with tag %d!\n", op->tag); return 0;
  }
}

#endif
