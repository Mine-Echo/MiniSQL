#include "index/index_iterator.h"

#include "index/basic_comparator.h"
#include "index/generic_key.h"

IndexIterator::IndexIterator() = default;

IndexIterator::IndexIterator(page_id_t page_id, BufferPoolManager *bpm, int index)
    : current_page_id(page_id), item_index(index), buffer_pool_manager(bpm) {
  if(page_id!=INVALID_PAGE_ID)
    page = reinterpret_cast<LeafPage *>(buffer_pool_manager->FetchPage(current_page_id)->GetData());
  else{
    page= nullptr;
    index=0;//Invalid的话把index都设置为0方便比较
  }

}

IndexIterator::~IndexIterator() {
  if (current_page_id != INVALID_PAGE_ID)
    buffer_pool_manager->UnpinPage(current_page_id, false);
}

std::pair<GenericKey *, RowId> IndexIterator::operator*() {
  ASSERT(page!= nullptr,"wrong");
  return this->page->GetItem(this->item_index);
}

IndexIterator &IndexIterator::operator++() {
  ASSERT(page!= nullptr,"wrong");
  if(this->item_index<page->GetSize()-1) item_index++;
  else{
    page_id_t pre_page_id=this->current_page_id;//原来的page_id
    this->current_page_id=page->GetNextPageId();
    buffer_pool_manager->UnpinPage(pre_page_id,false);
    if(this->current_page_id!=INVALID_PAGE_ID)
      page = reinterpret_cast<LeafPage *>(buffer_pool_manager->FetchPage(current_page_id));
    else page= nullptr;
    this->item_index=0;
  }
  return *this;
}

bool IndexIterator::operator==(const IndexIterator &itr) const {
  return current_page_id == itr.current_page_id && item_index == itr.item_index;
}

bool IndexIterator::operator!=(const IndexIterator &itr) const {
  return !(*this == itr);
}