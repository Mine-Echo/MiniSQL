#include "storage/table_heap.h"

/**
 * TODO: Student Implement
 */
bool TableHeap::InsertTuple(Row &row, Transaction *txn) {
  //If the tuple is too large (>= page_size), return false.
  if(row.GetSerializedSize(this->schema_)>TablePage::SIZE_MAX_ROW) return false;
  auto page=reinterpret_cast<TablePage*>(this->buffer_pool_manager_->FetchPage(this->first_page_id_));
  page_id_t next_page_id;
  while(1) {
    if (page == nullptr) return false;  // If the page could not be found, then abort the transaction.
    page->WLatch();
    if(page->InsertTuple(row,this->schema_,txn,this->lock_manager_,this->log_manager_)){
      page->WUnlatch();
      this->buffer_pool_manager_->UnpinPage(page->GetTablePageId(),true);
      return true;
    };
    page->WUnlatch();
    page_id_t page_id=page->GetPageId();
    next_page_id= page->GetNextPageId();
    if(next_page_id==INVALID_PAGE_ID){//说明是最后的page
      break;
    }else page=reinterpret_cast<TablePage*>(this->buffer_pool_manager_->FetchPage(next_page_id));
    this->buffer_pool_manager_->UnpinPage(page_id,true);//失败也要Unpin，因为Fetch成功就会Pin
  }
  auto new_page=reinterpret_cast<TablePage*>(this->buffer_pool_manager_->NewPage(next_page_id));
  if(new_page==nullptr) return false;//申请不到
  new_page->WLatch();
  new_page->Init(next_page_id,page->GetPageId(),this->log_manager_,txn);
  new_page->InsertTuple(row,this->schema_,txn,this->lock_manager_,log_manager_);
  new_page->WUnlatch();
  this->buffer_pool_manager_->UnpinPage(next_page_id,true);
  page->WLatch();
  page->SetNextPageId(next_page_id);
  page->WUnlatch();
  this->buffer_pool_manager_->UnpinPage(page->GetPageId(),true);
  return true;

}

bool TableHeap::MarkDelete(const RowId &rid, Transaction *txn) {
  // Find the page which contains the tuple.
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
  // If the page could not be found, then abort the transaction.
  if (page == nullptr) {
    return false;
  }
  // Otherwise, mark the tuple as deleted.
  page->WLatch();
  page->MarkDelete(rid, txn, lock_manager_, log_manager_);
  page->WUnlatch();
  buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);
  return true;
}

/**
 * TODO: Student Implement
 */
bool TableHeap::UpdateTuple(Row &row, const RowId &rid, Transaction *txn) {
  auto page=reinterpret_cast<TablePage*>(this->buffer_pool_manager_->FetchPage(rid.GetPageId()));
  if(page== nullptr) return false;//没找到
  Row old_row(rid);
  if(this->GetTuple(&old_row,txn)){
    page->WLatch();
    int type=page->UpdateTuple(row,&old_row,schema_,txn,lock_manager_,log_manager_);
    page->WUnlatch();
    switch(type){
      case 1:
      case 2:break;
      case 0:
        this->buffer_pool_manager_->UnpinPage(page->GetPageId(), true);
        row.SetRowId(rid);//设置rowid
        return true;
      case 3:
        //应该delete and insert
        this->buffer_pool_manager_->UnpinPage(page->GetPageId(),false);
        this->ApplyDelete(rid,txn);
        this->InsertTuple(row,txn);
        return true;
    }
  }
  this->buffer_pool_manager_->UnpinPage(page->GetPageId(),false);
  return false;
}

/**
 * TODO: Student Implement
 */
void TableHeap::ApplyDelete(const RowId &rid, Transaction *txn) {
  // Step1: Find the page which contains the tuple.
  auto page=reinterpret_cast<TablePage*>(this->buffer_pool_manager_->FetchPage(rid.GetPageId()));
  assert(page!= nullptr);
  // Step2: Delete the tuple from the page.
  page->WLatch();
  page->ApplyDelete(rid,txn,log_manager_);
  page->WUnlatch();
  this->buffer_pool_manager_->UnpinPage(page->GetPageId(),true);
}

void TableHeap::RollbackDelete(const RowId &rid, Transaction *txn) {
  // Find the page which contains the tuple.
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
  assert(page != nullptr);
  // Rollback to delete.
  page->WLatch();
  page->RollbackDelete(rid, txn, log_manager_);
  page->WUnlatch();
  buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);
}

/**
 * TODO: Student Implement
 */
bool TableHeap::GetTuple(Row *row, Transaction *txn) {
  auto page=reinterpret_cast<TablePage*>(this->buffer_pool_manager_->FetchPage(row->GetRowId().GetPageId()));
  if(page==nullptr) return false;
  page->RLatch();
  if(page->GetTuple(row,schema_,txn,lock_manager_)){
    page->RUnlatch();
    this->buffer_pool_manager_->UnpinPage(page->GetPageId(),false);
    return true;
  }
  page->RUnlatch();
  this->buffer_pool_manager_->UnpinPage(page->GetPageId(),false);
  return false;
}

void TableHeap::DeleteTable(page_id_t page_id) {
  if (page_id != INVALID_PAGE_ID) {
    auto temp_table_page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(page_id));  // 删除table_heap
    if (temp_table_page->GetNextPageId() != INVALID_PAGE_ID)
      DeleteTable(temp_table_page->GetNextPageId());
    buffer_pool_manager_->UnpinPage(page_id, false);
    buffer_pool_manager_->DeletePage(page_id);
  } else {
    DeleteTable(first_page_id_);
  }
}

/**
 * TODO: Student Implement
 */
TableIterator TableHeap::Begin(Transaction *txn) {
  RowId rid=INVALID_ROWID;
  if(this->first_page_id_!=INVALID_PAGE_ID){
    auto page=reinterpret_cast<TablePage*>(this->buffer_pool_manager_->FetchPage(first_page_id_));
    ASSERT(page!= nullptr,"can't fetch page from disk");
    page->GetFirstTupleRid(&rid);//获取成功会改变rid，否则rid还是会被设置为INVALID_ROWID
  }
  return TableIterator(this,rid);
}

/**
 * TODO: Student Implement
 */
TableIterator TableHeap::End() {
  return TableIterator(this,INVALID_ROWID);
}
