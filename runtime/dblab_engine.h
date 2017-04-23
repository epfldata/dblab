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

struct scanop_t {
  numeric_int_t tag;
  numeric_int_t table_size;
  numeric_int_t counter;
  numeric_int_t record_size;
  record_t table;
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

// TODO
void aggop_open(struct operator_t* op) {
  struct operator_t* parent = op->parent;
  operator_open(parent);
}

// TODO
record_t aggop_next(struct operator_t* op) {
  struct operator_t* parent = op->parent;
  return operator_next(parent);
}

// TODO
void scanop_open(struct operator_t* op) {
  struct scanop_t* scanop = (struct scanop_t*)op;
  printf("scan op relation of size %d\n", scanop->table_size);
}

// TODO
record_t scanop_next(struct operator_t* op) {
  struct scanop_t* scanop = (struct scanop_t*)op;
  // printf("scan op next[%lld] of %lld\n", scanop->counter, scanop->table_size);
  if (scanop->counter < scanop->table_size) {
    record_t v = scanop->table + scanop->counter * scanop->record_size;
    scanop->counter += 1;
    return v;
  } else {
    return NULL;
  }
}

// TODO
void printop_open(struct operator_t* op) {
  struct operator_t* parent = op->parent;
  operator_open(parent);
}

// TODO limit and printFunc functions!
record_t printop_next(struct operator_t* op) {
  struct operator_t* parent = op->parent;
  int numRows = 0;
  boolean_t exit = false;
  while (!exit) {
    record_t t = operator_next(parent);
    if(t == NULL) {
      exit = true;
    } else {
      // TODO
      numRows += 1;
    }
      // if ((limit != -1 && numRows >= limit) || t == NullDynamicRecord) exit = true
      // else { printFunc(t); numRows += 1 }
  }
  printf("(%d rows)\n", numRows);
  return 0;
}

// TODO
void printop_run(void* raw_op) {
  struct operator_t* op = raw_op;
  printop_open(op);
  printop_next(op);
}

// TODO
void operator_open(struct operator_t* op) {  
  switch(op->tag) {
    case AGG_OP_TAG: aggop_open(op); break;
    case PRINT_OP_TAG: printop_open(op); break;
    case SCAN_OP_TAG: scanop_open(op); break;
    case SELECT_OP_TAG: selectop_open(op); break;
    default: printf("Default Open with tag %d!\n", op->tag);
  }
}

// TODO
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
