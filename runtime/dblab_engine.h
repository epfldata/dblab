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

#define AGG_OP_TAG 1
#define PRINT_OP_TAG 2 

struct operator_t {
  int tag;
  void* parent;
};

GList** Seq(void* e1) {
	GList* result = NULL;
	result = g_list_append (result, e1);
	return result;
}

void operator_open(struct operator_t* op) {
  printf("Open with tag %d!\n", op->tag);
}

void operator_next(struct operator_t* op) {
  printf("Next with tag %d!\n", op->tag);
}

void printop_open(struct operator_t* op) {
  struct operator_t* parent = op->parent;
  operator_open(parent);
}

void printop_next(struct operator_t* op) {
  struct operator_t* parent = op->parent;
  operator_next(parent);
  // FIXME implement the while loop
  printf("(%d rows)\n", 0);
}

void printop_run(void* raw_op) {
  struct operator_t* op = raw_op;
  printop_open(op);
  printop_next(op);
}

#endif
