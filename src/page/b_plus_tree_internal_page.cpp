#include "page/b_plus_tree_internal_page.h"

#include "index/generic_key.h"

#define pairs_off (data_)
#define pair_size (GetKeySize() + sizeof(page_id_t))
#define key_off 0
#define val_off GetKeySize()

/**
 * TODO: Student Implement
 */
/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/
/*
 * Init method after creating a new internal page
 * Including set page type, set current size, set page id, set parent id and set
 * max page size
 */
void InternalPage::Init(page_id_t page_id, page_id_t parent_id, int key_size, int max_size) {
  this->SetPageType(IndexPageType::INTERNAL_PAGE);
  this->SetKeySize(key_size);
  this->SetMaxSize(max_size);
  this->SetPageId(page_id);
  this->SetParentPageId(parent_id);
  this->SetSize(0);
}
/*
 * Helper method to get/set the key associated with input "index"(a.k.a
 * array offset)
 */
GenericKey *InternalPage::KeyAt(int index) {
  return reinterpret_cast<GenericKey *>(pairs_off + index * pair_size + key_off);
}

void InternalPage::SetKeyAt(int index, GenericKey *key) {
  memcpy(pairs_off + index * pair_size + key_off, key, GetKeySize());
}

page_id_t InternalPage::ValueAt(int index) const {
  return *reinterpret_cast<const page_id_t *>(pairs_off + index * pair_size + val_off);
}

void InternalPage::SetValueAt(int index, page_id_t value) {
  *reinterpret_cast<page_id_t *>(pairs_off + index * pair_size + val_off) = value;
}

int InternalPage::ValueIndex(const page_id_t &value) const {
  for (int i = 0; i < GetSize(); ++i) {
    if (ValueAt(i) == value)
      return i;
  }
  return -1;
}

void *InternalPage::PairPtrAt(int index) {
  return KeyAt(index);
}

void InternalPage::PairCopy(void *dest, void *src, int pair_num) {
  memcpy(dest, src, pair_num * (GetKeySize() + sizeof(page_id_t)));
}
/*****************************************************************************
 * LOOKUP
 *****************************************************************************/
/*
 * Find and return the child pointer(page_id) which points to the child page
 * that contains input "key"
 * Start the search from the second key(the first key should always be invalid)
 * 用了二分查找
 */
page_id_t InternalPage::Lookup(const GenericKey *key, const KeyManager &KM) {
  int right=this->GetSize()-1,left=1,mid;
  while(left<=right){
    mid=(left+right)/2;
    int type=KM.CompareKeys(key,this->KeyAt(mid));
    if(type==0) return this->ValueAt(mid);
    else if(type<0) right=mid-1;
    else left=mid+1;
  }
  //right是小于key中最大的一个
  return this->ValueAt(right);
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Populate new root page with old_value + new_key & new_value
 * When the insertion cause overflow from leaf page all the way upto the root
 * page, you should create a new root page and populate its elements.
 * NOTE: This method is only called within InsertIntoParent()(b_plus_tree.cpp)
 */
void InternalPage::PopulateNewRoot(const page_id_t &old_value, GenericKey *new_key, const page_id_t &new_value) {
  this->SetSize(2);//FIXME:两个指针?
  this->SetValueAt(0,old_value);
  this->SetKeyAt(1,new_key);
  this->SetValueAt(1,new_value);
}

/*
 * Insert new_key & new_value pair right after the pair with its value ==
 * old_value
 * @return:  new size after insertion
 */
int InternalPage::InsertNodeAfter(const page_id_t &old_value, GenericKey *new_key, const page_id_t &new_value) {
  int index=this->ValueIndex(old_value);
  //后移
  this->PairCopy(PairPtrAt(index+2), PairPtrAt(index+1),this->GetSize()-(index+1));
  this->SetKeyAt(index+1,new_key);
  this->SetValueAt(index+1,new_value);
  this->IncreaseSize(1);
  return this->GetSize();
}

/*****************************************************************************
 * SPLIT
 *****************************************************************************/
/*
 * Remove half of key & value pairs from this page to "recipient" page
 * buffer_pool_manager 是干嘛的？传给CopyNFrom()用于Fetch数据页
 */
void InternalPage::MoveHalfTo(InternalPage *recipient, BufferPoolManager *buffer_pool_manager) {
  int half=this->GetSize()/2;
  int remain=GetSize()-half;
  recipient->CopyNFrom(this->PairPtrAt(remain),half,buffer_pool_manager);
  this->SetSize(remain);//设置Size
}

/* Copy entries into me, starting from {items} and copy {size} entries.
 * Since it is an internal page, for all entries (pages) moved, their parents page now changes to me.
 * So I need to 'adopt' them by changing their parent page id, which needs to be persisted with BufferPoolManger
 *
 */
void InternalPage::CopyNFrom(void *src, int size, BufferPoolManager *buffer_pool_manager) {
//  this->SetSize(size);
  int pre_size=this->GetSize();
  this->IncreaseSize(size);//FIXME:应该增加size?,但不能超过max_size；还是设置为size？不过前者可以兼容后者
  this->PairCopy(PairPtrAt(pre_size),src,size);
  for(int i=0;i<size;i++){
    Page* page=buffer_pool_manager->FetchPage(ValueAt(i+pre_size));
    BPlusTreePage* bp_page=reinterpret_cast<BPlusTreePage*>(page->GetData());
    bp_page->SetParentPageId(this->GetPageId());
    buffer_pool_manager->UnpinPage(bp_page->GetPageId(),true);
  }

}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Remove the key & value pair in internal page according to input index(a.k.a
 * array offset)
 * NOTE: store key&value pair continuously after deletion
 */
void InternalPage::Remove(int index) {
  //把后面的往前移
  this->PairCopy(this->PairPtrAt(index),this->PairPtrAt(index+1),this->GetSize()-(index+1));
  this->IncreaseSize(-1);
}

/*
 * Remove the only key & value pair in internal page and return the value
 * NOTE: only call this method within AdjustRoot()(in b_plus_tree.cpp)
 */
page_id_t InternalPage::RemoveAndReturnOnlyChild() {
  this->SetSize(0);
  return ValueAt(0);
}

/*****************************************************************************
 * MERGE
 *****************************************************************************/
/*
 * Remove all key & value pairs from this page to "recipient" page.
 * The middle_key is the separation key you should get from the parent. You need
 * to make sure the middle key is added to the recipient to maintain the invariant.
 * You also need to use BufferPoolManager to persist changes to the parent page id for those
 * pages that are moved to the recipient
 */
void InternalPage::MoveAllTo(InternalPage *recipient, GenericKey *middle_key, BufferPoolManager *buffer_pool_manager) {
  this->SetKeyAt(0,middle_key);
  recipient->CopyNFrom(this->PairPtrAt(0),this->GetSize(),buffer_pool_manager);
  this->SetSize(0);
}

/*****************************************************************************
 * REDISTRIBUTE
 *****************************************************************************/
/*
 * Remove the first key & value pair from this page to tail of "recipient" page.
 *
 * The middle_key is the separation key you should get from the parent. You need
 * to make sure the middle key is added to the recipient to maintain the invariant.
 * You also need to use BufferPoolManager to persist changes to the parent page id for those
 * pages that are moved to the recipient
 */
void InternalPage::MoveFirstToEndOf(InternalPage *recipient, GenericKey *middle_key,
                                    BufferPoolManager *buffer_pool_manager) {
  this->SetKeyAt(0,middle_key);
  //将第一个接到末尾
//  recipient->CopyNFrom(this->PairPtrAt(0),1,buffer_pool_manager);
  recipient->CopyLastFrom(middle_key,this->ValueAt(0),buffer_pool_manager);
  //移除第一个
  this->PairCopy(PairPtrAt(0), PairPtrAt(1),this->GetSize()-1);
  //修改size
  this->IncreaseSize(-1);
}

/* Append an entry at the end.
 * Since it is an internal page, the moved entry(page)'s parent needs to be updated.
 * So I need to 'adopt' it by changing its parent page id, which needs to be persisted with BufferPoolManger
 */
void InternalPage::CopyLastFrom(GenericKey *key, const page_id_t value, BufferPoolManager *buffer_pool_manager) {
  //copy
  this->SetKeyAt(this->GetSize(),key);
  this->SetValueAt(this->GetSize(),value);
  //set size
  this->IncreaseSize(1);
  //change parent_page_id
  Page* page=reinterpret_cast<Page*>(buffer_pool_manager->FetchPage(value));
  auto bp_page=reinterpret_cast<BPlusTreePage*>(page->GetData());
  bp_page->SetParentPageId(this->GetPageId());
  buffer_pool_manager->UnpinPage(value,true);
}

/*
 * Remove the last key & value pair from this page to head of "recipient" page.
 * You need to handle the original dummy key properly, e.g. updating recipient’s array to position the middle_key at the
 * right place.
 * You also need to use BufferPoolManager to persist changes to the parent page id for those pages that are
 * moved to the recipient
 */
void InternalPage::MoveLastToFrontOf(InternalPage *recipient, GenericKey *middle_key,
                                     BufferPoolManager *buffer_pool_manager) {
  recipient->SetKeyAt(0,middle_key);
  //往后移
  recipient->CopyFirstFrom(this->ValueAt(this->GetSize()-1),buffer_pool_manager);
  //size--，也相当于删除尾部
  this->IncreaseSize(-1);
}

/* Append an entry at the beginning.
 * Since it is an internal page, the moved entry(page)'s parent needs to be updated.
 * So I need to 'adopt' it by changing its parent page id, which needs to be persisted with BufferPoolManger
 */
void InternalPage::CopyFirstFrom(const page_id_t value, BufferPoolManager *buffer_pool_manager) {
  //后移
  this->PairCopy(PairPtrAt(1), PairPtrAt(0),this->GetSize());
  //插入最前面
  this->SetValueAt(0,value);
  //改变parant_page_id
  Page* page=reinterpret_cast<Page*>(buffer_pool_manager->FetchPage(value));
  auto bp_page=reinterpret_cast<BPlusTreePage*>(page->GetData());
  bp_page->SetParentPageId(this->GetPageId());
  buffer_pool_manager->UnpinPage(value,true);
  //size++
  this->IncreaseSize(1);
}