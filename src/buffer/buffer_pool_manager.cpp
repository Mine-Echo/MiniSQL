#include "buffer/buffer_pool_manager.h"

#include "glog/logging.h"
#include "page/bitmap_page.h"

static const char EMPTY_PAGE_DATA[PAGE_SIZE] = {0};

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager) {
  pages_ = new Page[pool_size_];
  replacer_ = new LRUReplacer(pool_size_);
  for (size_t i = 0; i < pool_size_; i++) {
    free_list_.emplace_back(i);
  }
}

BufferPoolManager::~BufferPoolManager() {
  for (auto page : page_table_) {
    FlushPage(page.first);
  }
  delete[] pages_;
  delete replacer_;
}

/**
 * TODO: Student Implement
 */
Page *BufferPoolManager::FetchPage(page_id_t page_id) {
  // 1.     Search the page table for the requested page (P).
  // 1.1    If P exists, pin it and return it immediately.
  // 1.2    If P does not exist, find a replacement page (R) from either the free list or the replacer.
  //        Note that pages are always found from the free list first.
  // 2.     If R is dirty, write it back to the disk.
  // 3.     Delete R from the page table and insert P.
  // 4.     Update P's metadata, read in the page content from disk, and then return a pointer to P.
  if(this->page_table_.find(page_id)!=page_table_.end()){
    frame_id_t P=page_table_[page_id];
    this->replacer_->Pin(P);//从replacer中删除
    pages_[P].pin_count_++;
    return pages_+P;
  }
  else{
    frame_id_t R;
    if(this->free_list_.size()>0){//先从free_list里面找
      R=free_list_.front();
      free_list_.pop_front();
    }else if(this->replacer_->Victim(&R)){}
    else return nullptr;
    if(pages_[R].IsDirty()) //dirty的话写进磁盘
      disk_manager_->WritePage(pages_[R].page_id_,pages_[R].GetData());
    page_table_.erase(page_id);
    page_table_.insert(pair<page_id_t ,frame_id_t>(page_id,R));
//    pages_[R].ResetMemory();//没必要
    pages_[R].page_id_=page_id;
    pages_[R].pin_count_=1;
    pages_[R].is_dirty_=false;
    disk_manager_->ReadPage(page_id,pages_[R].GetData());
    replacer_->Pin(R);//从replacer之中删除,好像没有必要
    return pages_+R;
  }
}

/**
 * TODO: Student Implement
 */
Page *BufferPoolManager::NewPage(page_id_t &page_id) {
  // 0.   Make sure you call AllocatePage!
  // 1.   If all the pages in the buffer pool are pinned, return nullptr.
  // 2.   Pick a victim page P from either the free list or the replacer. Always pick from the free list first.
  // 3.   Update P's metadata, zero out memory and add P to the page table.
  // 4.   Set the page ID output parameter. Return a pointer to P.
  frame_id_t P;
  if(free_list_.size()>0){
    page_id=AllocatePage();
    if(page_id!=INVALID_PAGE_ID){
      P=free_list_.front();
      free_list_.pop_front();
    }else return nullptr;//磁盘空间不足，申请失败
  }else if(replacer_->Size()>0){
    page_id=this->AllocatePage();
    if(page_id!=INVALID_PAGE_ID)
      replacer_->Victim(&P);
    else return nullptr;//磁盘空间不足，申请失败
  }else return nullptr;//buffer pool的全被Pinned，申请失败
  if(pages_[P].is_dirty_) this->disk_manager_->WritePage(pages_[P].page_id_,pages_[P].GetData());
  page_table_.erase(pages_[P].page_id_);//从表中删除
  page_table_.insert(pair<page_id_t,frame_id_t>(page_id,P));
  pages_[P].ResetMemory();
  pages_[P].is_dirty_=false;
  pages_[P].pin_count_=1;
  pages_[P].page_id_=page_id;
  replacer_->Pin(P);//从replacer之中删除,可能没有必要
  return pages_+P;
}

/**
 * TODO: Student Implement
 */
bool BufferPoolManager::DeletePage(page_id_t page_id) {
  // 0.   Make sure you call DeallocatePage!
  // 1.   Search the page table for the requested page (P).
  // 1.   If P does not exist, return true.
  // 2.   If P exists, but has a non-zero pin-count, return false. Someone is using the page.
  // 3.   Otherwise, P can be deleted. Remove P from the page table, reset its metadata and return it to the free list.
  if (this->page_table_.find(page_id) == page_table_.end()) {
    // 内存中没找到
    this->DeallocatePage(page_id);
    return true;
  } else {
    frame_id_t P = page_table_[page_id];
    if (pages_[P].pin_count_ > 0)
      return false;  // 有人在使用
    else {
//      this->DeletePage(page_id);  // 从磁盘中清除
      this->DeallocatePage(page_id);
      pages_[P].ResetMemory();    // 内存中清除
      pages_->is_dirty_ = false;
      this->replacer_->Pin(P);     // 从replacer中清除
      page_table_.erase(page_id);  // 在table中清除
      free_list_.push_back(P);     // 添加到free_list
      return true;
    }
  }
}

/**
 * TODO: Student Implement
 */
bool BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty) {
  if(this->page_table_.find(page_id)==page_table_.end()) return false;//内存中没有这个page
  frame_id_t P=page_table_[page_id];
  pages_[P].is_dirty_|=is_dirty;
  if((--pages_[P].pin_count_)==0) replacer_->Unpin(P);
  return true;
}

/**
 * TODO: Student Implement
 */
bool BufferPoolManager::FlushPage(page_id_t page_id) {
  if(page_table_.find(page_id)==page_table_.end()) return false;//内存中没有这页
  frame_id_t P=page_table_[page_id];
  if(pages_[P].is_dirty_) {
    pages_[P].is_dirty_=false;//更新dirty
    disk_manager_->WritePage(pages_[P].page_id_, pages_[P].GetData());
  }
  return true;
}

page_id_t BufferPoolManager::AllocatePage() {
  int next_page_id = disk_manager_->AllocatePage();
  return next_page_id;
}

void BufferPoolManager::DeallocatePage(__attribute__((unused)) page_id_t page_id) {
  disk_manager_->DeAllocatePage(page_id);
}

bool BufferPoolManager::IsPageFree(page_id_t page_id) {
  return disk_manager_->IsPageFree(page_id);
}

// Only used for debug
bool BufferPoolManager::CheckAllUnpinned() {
  bool res = true;
  for (size_t i = 0; i < pool_size_; i++) {
    if (pages_[i].pin_count_ != 0) {
      res = false;
      LOG(ERROR) << "page " << pages_[i].page_id_ << " pin count:" << pages_[i].pin_count_ << endl;
    }
  }
  return res;
}