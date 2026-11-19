#include "storage/table_iterator.h"

#include <stdexcept>

#include "storage/table_heap.h"

TableIterator::TableIterator(TableHeap *table_heap, RowId rid) : table_heap_(table_heap), row_(rid) {
  if (rid.GetPageId() != INVALID_PAGE_ID) {
    table_heap_->GetTuple(&row_, nullptr);
  }
}

bool TableIterator::operator==(const TableIterator &itr) const {
  return table_heap_ == itr.table_heap_ && row_.GetRowId() == itr.row_.GetRowId();
}

bool TableIterator::operator!=(const TableIterator &itr) const { return !(*this == itr); }

const Row &TableIterator::operator*() { return row_; }

Row *TableIterator::operator->() { return &row_; }

TableIterator &TableIterator::operator=(const TableIterator &itr) {
  if (this != &itr) {
    row_ = itr.row_;
    table_heap_ = itr.table_heap_;
  }
  return *this;
}

TableIterator &TableIterator::operator++() {
  const RowId current = row_.GetRowId();
  page_id_t page_id = current.GetPageId();
  RowId next = INVALID_ROWID;
  while (page_id != INVALID_PAGE_ID) {
    auto *buffer_pool = table_heap_->buffer_pool_manager_;
    auto *page = reinterpret_cast<TablePage *>(buffer_pool->FetchPage(page_id));
    if (page == nullptr) {
      throw std::runtime_error("Failed to fetch table page while advancing iterator");
    }
    page->RLatch();
    const bool found = page_id == current.GetPageId() ? page->GetNextTupleRid(current, &next)
                                                      : page->GetFirstTupleRid(&next);
    const page_id_t next_page_id = page->GetNextPageId();
    page->RUnlatch();
    buffer_pool->UnpinPage(page_id, false);
    if (found) {
      break;
    }
    page_id = next_page_id;
  }

  row_ = Row(next);
  if (next.GetPageId() != INVALID_PAGE_ID) {
    table_heap_->GetTuple(&row_, nullptr);
  }
  return *this;
}

TableIterator TableIterator::operator++(int) {
  TableIterator previous(*this);
  ++(*this);
  return previous;
}
