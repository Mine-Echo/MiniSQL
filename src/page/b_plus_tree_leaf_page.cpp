#include "page/b_plus_tree_leaf_page.h"

#include <algorithm>

#include "index/generic_key.h"

#define pairs_off (data_)
#define pair_size (GetKeySize() + sizeof(RowId))
#define key_off 0
#define val_off GetKeySize()
/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/

/**
 * TODO: Student Implement
 */
/**
 * Init method after creating a new leaf page
 * Including set page type, set current size to zero, set page id/parent id, set
 * next page id and set max size
 * 未初始化next_page_id
 */
void LeafPage::Init(page_id_t page_id, page_id_t parent_id, int key_size, int max_size) {
  this->SetPageType(IndexPageType::LEAF_PAGE);
  this->SetPageId(page_id);
  this->SetParentPageId(parent_id);
  this->SetKeySize(key_size);
  this->SetMaxSize(max_size);
  this->SetSize(0);
  this->SetNextPageId(INVALID_PAGE_ID);//FIXME:我认为可以初始化为INVALID
}

/**
 * Helper methods to set/get next page id
 */
page_id_t LeafPage::GetNextPageId() const {
  return next_page_id_;
}

void LeafPage::SetNextPageId(page_id_t next_page_id) {
  next_page_id_ = next_page_id;
  if (next_page_id == 0) {
    LOG(INFO) << "Fatal error";
  }
}

/**
 * TODO: Student Implement
 */
/**
 * Helper method to find the first index i so that pairs_[i].first >= key
 * NOTE: This method is only used when generating index iterator
 * 二分查找
 */
int LeafPage::KeyIndex(const GenericKey *key, const KeyManager &KM) {
  //FIXME:为什么是大于等于
  int left=0,right=this->GetSize()-1,mid;
  while(left<=right){
    mid=(left+right)/2;
    int type=KM.CompareKeys(key,this->KeyAt(mid));
    if(type==0)return mid;
    else if(type<0) right=mid-1;
    else if(type>0)left=mid+1;
  }
  if(left>=this->GetSize()) return -1;//没有找到大于等于key的
  else return left;
}

/*
 * Helper method to find and return the key associated with input "index"(a.k.a
 * array offset)
 */
GenericKey *LeafPage::KeyAt(int index) {
  return reinterpret_cast<GenericKey *>(pairs_off + index * pair_size + key_off);
}

void LeafPage::SetKeyAt(int index, GenericKey *key) {
  memcpy(pairs_off + index * pair_size + key_off, key, GetKeySize());
}

RowId LeafPage::ValueAt(int index) const {
  return *reinterpret_cast<const RowId *>(pairs_off + index * pair_size + val_off);
}

void LeafPage::SetValueAt(int index, RowId value) {
  *reinterpret_cast<RowId *>(pairs_off + index * pair_size + val_off) = value;
}

void *LeafPage::PairPtrAt(int index) {
  return KeyAt(index);
}

void LeafPage::PairCopy(void *dest, void *src, int pair_num) {
  memcpy(dest, src, pair_num * (GetKeySize() + sizeof(RowId)));
}

/*
 * Helper method to find and return the key & value pair associated with input
 * "index"(a.k.a. array offset)
 */
std::pair<GenericKey *, RowId> LeafPage::GetItem(int index) {
  // replace with your own code

  return make_pair(KeyAt(index), ValueAt(index));
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert key & value pair into leaf page ordered by key
 * @return page size after insertion
 */
int LeafPage::Insert(GenericKey *key, const RowId &value, const KeyManager &KM) {
  int left=0,right=this->GetSize()-1,mid;
  while(left<=right){
    mid=(left+right)/2;
    int type=KM.CompareKeys(key,this->KeyAt(mid));
    if(type==0) ASSERT(1==0,"Insert Wrong");//不可能找到还没插入的key，如果找到说明有错误直接终止
    else if(type<0) right=mid-1;
    else if(type>0)left=mid+1;
  }
  //left就是要插入的位置
  if(left!=this->GetSize()){
    //将右边的右移
    this->PairCopy(PairPtrAt(left+1), PairPtrAt(left),this->GetSize()-left);
  }
  //插入
  this->SetKeyAt(left,key);
  this->SetValueAt(left,value);
  //增加size
  this->IncreaseSize(1);
  return this->GetSize();
}

/*****************************************************************************
 * SPLIT
 *****************************************************************************/
/*
 * Remove half of key & value pairs from this page to "recipient" page
 */
void LeafPage::MoveHalfTo(LeafPage *recipient) {
  int half=this->GetSize()/2;
  int remain=this->GetSize()-half;
  recipient->CopyNFrom(this->PairPtrAt(remain),half);
  //更改size
  this->IncreaseSize(-half);
}

/*
 * Copy starting from items, and copy {size} number of elements into me.
 */
void LeafPage::CopyNFrom(void *src, int size) {
  int pre_size=this->GetSize();
  this->IncreaseSize(size);
  this->PairCopy(PairPtrAt(pre_size),src,size);
  //因为是leafPage，所以不用更改子节点的parent
}

/*****************************************************************************
 * LOOKUP
 *****************************************************************************/
/*
 * For the given key, check to see whether it exists in the leaf page. If it
 * does, then store its corresponding value in input "value" and return true.
 * If the key does not exist, then return false
 */
bool LeafPage::Lookup(const GenericKey *key, RowId &value, const KeyManager &KM) {
  int index=this->KeyIndex(key,KM);
  //没找到
  if(index==-1|| KM.CompareKeys(KeyAt(index),key)!=0) return false;
  //找到了，更改value
  value=this->ValueAt(index);
  return true;
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * First look through leaf page to see whether delete key exist or not. If
 * existed, perform deletion, otherwise return immediately.
 * NOTE: store key&value pair continuously after deletion
 * @return  page size after deletion
 */
int LeafPage::RemoveAndDeleteRecord(const GenericKey *key, const KeyManager &KM) {
  int index=this->KeyIndex(key,KM);
  if(index==-1||(KM.CompareKeys(KeyAt(index),key)!=0)) return this->GetSize();//没找到
  //找到了，将后面的元素前移，然后size--
  this->PairCopy(PairPtrAt(index), PairPtrAt(index+1),this->GetSize()-(index+1));
  this->IncreaseSize(-1);
  return this->GetSize();
}

/*****************************************************************************
 * MERGE
 *****************************************************************************/
/*
 * Remove all key & value pairs from this page to "recipient" page. Don't forget
 * to update the next_page id in the sibling page
 */
void LeafPage::MoveAllTo(LeafPage *recipient) {
  recipient->CopyNFrom(this->PairPtrAt(0),this->GetSize());
  recipient->SetNextPageId(this->GetNextPageId());
}

/*****************************************************************************
 * REDISTRIBUTE
 *****************************************************************************/
/*
 * Remove the first key & value pair from this page to "recipient" page.
 *
 */
void LeafPage::MoveFirstToEndOf(LeafPage *recipient) {
  //recipient
  recipient->CopyLastFrom(this->KeyAt(0),this->ValueAt(0));
  //后面的前移
  this->IncreaseSize(-1);
  this->PairCopy(PairPtrAt(0), PairPtrAt(1),this->GetSize());
}

/*
 * Copy the item into the end of my item list. (Append item to my array)
 */
void LeafPage::CopyLastFrom(GenericKey *key, const RowId value) {
  this->SetKeyAt(this->GetSize(),key);
  this->SetValueAt(this->GetSize(),value);
  this->IncreaseSize(1);
}

/*
 * Remove the last key & value pair from this page to "recipient" page.
 */
void LeafPage::MoveLastToFrontOf(LeafPage *recipient) {
  recipient->CopyFirstFrom(this->KeyAt(this->GetSize()-1), ValueAt(GetSize()-1));
  this->IncreaseSize(-1);
}

/*
 * Insert item at the front of my items. Move items accordingly.
 *
 */
void LeafPage::CopyFirstFrom(GenericKey *key, const RowId value) {
  this->PairCopy(PairPtrAt(1), PairPtrAt(0),this->GetSize());//后移
  this->SetKeyAt(0,key);
  this->SetValueAt(0,value);
  IncreaseSize(1);
}