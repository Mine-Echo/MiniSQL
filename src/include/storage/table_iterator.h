#ifndef MINISQL_TABLE_ITERATOR_H
#define MINISQL_TABLE_ITERATOR_H

#include "common/rowid.h"
#include "record/row.h"
#include "transaction/transaction.h"

class TableHeap;

class TableIterator {
 public:
  TableIterator() = default;
  explicit TableIterator(TableHeap *table_heap, RowId rid);
  TableIterator(const TableIterator &other) = default;
  virtual ~TableIterator() = default;

  bool operator==(const TableIterator &itr) const;
  bool operator!=(const TableIterator &itr) const;

  const Row &operator*();
  Row *operator->();

  TableIterator &operator=(const TableIterator &itr);
  TableIterator &operator++();
  TableIterator operator++(int);

 private:
  TableHeap *table_heap_{nullptr};
  Row row_{INVALID_ROWID};
};

#endif  // MINISQL_TABLE_ITERATOR_H
