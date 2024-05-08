#include "storage/table_iterator.h"

#include "common/macros.h"
#include "storage/table_heap.h"

/**
 * TODO: Student Implement
 */
TableIterator::TableIterator(TableHeap* tableHeap,RowId rid) {
  this->table_heap_ = tableHeap;
  //  this->row=new Row(rid);
  if (rid.GetPageId() != INVALID_PAGE_ID){  // 有效则读取数据
    this->row=new Row(rid);
    this->table_heap_->GetTuple(this->row, nullptr);
  }else
    this->row=new Row(INVALID_ROWID);
}

TableIterator::TableIterator(const TableIterator &other) {
  this->table_heap_=other.table_heap_;
  this->row=new Row(*other.row);
}

TableIterator::~TableIterator() {
  delete row;
}

bool TableIterator::operator==(const TableIterator &itr) const {
  if(this->table_heap_==itr.table_heap_&&this->row->GetRowId()==itr.row->GetRowId())
    return true;
  return false;
}

bool TableIterator::operator!=(const TableIterator &itr) const {
  return !this->operator==(itr);
}

const Row &TableIterator::operator*() {
//  ASSERT(false, "Not implemented yet.");
  return *row;
}

Row *TableIterator::operator->() {
  return row;
}

TableIterator &TableIterator::operator=(const TableIterator &itr) noexcept {
//  ASSERT(false, "Not implemented yet.");
  this->table_heap_=itr.table_heap_;
  this->row=new Row(*itr.row);
}

// ++iter
TableIterator &TableIterator::operator++() {
  if(this->row->GetRowId().GetPageId()!=INVALID_PAGE_ID) {//当前不是无效的page
    auto page=reinterpret_cast<TablePage *>(this->table_heap_->buffer_pool_manager_->FetchPage(row->GetRowId().GetPageId()));
    RowId new_rid;
    page->RLatch();
    if(page->GetNextTupleRid(row->GetRowId(),&new_rid)){//此page有next 元组
      page->RUnlatch();
      delete this->row;
      this->row=new Row(new_rid);
      this->table_heap_->GetTuple(row,nullptr);
    }else{//没有next 元组
      auto next_page_id=page->GetNextPageId();
      page->RUnlatch();
      if(next_page_id==INVALID_PAGE_ID){//当前是最后的page，下一页为无效
        delete this->row;
        this->row=new Row(INVALID_ROWID);

      }else{
        auto next_page=reinterpret_cast<TablePage *>(this->table_heap_->buffer_pool_manager_->FetchPage(next_page_id));
        next_page->RLatch();
        next_page->GetFirstTupleRid(&new_rid);
        next_page->RUnlatch();
        delete this->row;
        row=new Row(new_rid);
        this->table_heap_->GetTuple(row, nullptr);
      }
      table_heap_->buffer_pool_manager_->UnpinPage(next_page_id,false);
    }
    this->table_heap_->buffer_pool_manager_->UnpinPage(page->GetPageId(),false);
  }
  return *this;
}

// iter++
TableIterator TableIterator::operator++(int) {
  RowId rid=this->row->GetRowId();
  this->operator++();
  return TableIterator(this->table_heap_,rid);
}
