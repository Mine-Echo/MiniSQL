#include "index/b_plus_tree.h"

#include <string>

#include "glog/logging.h"
#include "index/basic_comparator.h"
#include "index/generic_key.h"
#include "page/index_roots_page.h"

/**
 * TODO: Student Implement
 */
BPlusTree::BPlusTree(index_id_t index_id, BufferPoolManager *buffer_pool_manager, const KeyManager &KM,
                     int leaf_max_size, int internal_max_size)
    : index_id_(index_id),
      buffer_pool_manager_(buffer_pool_manager),
      processor_(KM),
      leaf_max_size_(LEAF_PAGE_SIZE),
      internal_max_size_(INTERNAL_PAGE_SIZE) {
  Page* page=buffer_pool_manager->FetchPage(INDEX_ROOTS_PAGE_ID);
  auto index_roots_page=reinterpret_cast<IndexRootsPage*>(page->GetData());
  index_roots_page->GetRootId(index_id,&this->root_page_id_);
  buffer_pool_manager->UnpinPage(INDEX_ROOTS_PAGE_ID,false);
}

void BPlusTree::Destroy(page_id_t current_page_id) {
  //FIXME:不太清楚destroy要干嘛
  this->root_page_id_=INVALID_PAGE_ID;
}

/*
 * Helper function to decide whether current b+tree is empty
 */
bool BPlusTree::IsEmpty() const {
  return this->root_page_id_==INVALID_PAGE_ID;
}

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/*
 * Return the only value that associated with input key
 * This method is used for point query
 * @return : true means key exists
 */
bool BPlusTree::GetValue(const GenericKey *key, std::vector<RowId> &result, Transaction *transaction) {
  if(this->IsEmpty()) return false;
  BPlusTreeLeafPage* leaf_page=reinterpret_cast<BPlusTreeLeafPage*>(this->FindLeafPage(key));
  //遍历leaf_page
  RowId value;
  if(!leaf_page->Lookup(key,value,processor_)){//not exist
    this->buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(),false);
    return false;
  }
  this->buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(),false);
  result.push_back(value);
  return true;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert constant key & value pair into b+ tree
 * if current tree is empty, start new tree, update root page id and insert
 * entry, otherwise insert into leaf page.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
bool BPlusTree::Insert(GenericKey *key, const RowId &value, Transaction *transaction) {
  if(this->IsEmpty()){//数空
    this->StartNewTree(key,value);
  }else{//树不空
    if(!this->InsertIntoLeaf(key,value,transaction)) return false;
  }
  return true;
}
/*
 * Insert constant key & value pair into an empty tree
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then update b+
 * tree's root page id and insert entry directly into leaf page.
 */
void BPlusTree::StartNewTree(GenericKey *key, const RowId &value) {
  //获得一块新的page
  Page* page=this->buffer_pool_manager_->NewPage(this->root_page_id_);
  ASSERT(page!= nullptr,"out of memory");
  BPlusTreeLeafPage* root_page=reinterpret_cast<BPlusTreeLeafPage*>(page->GetData());
  //初始化
  root_page->Init(root_page_id_,INVALID_PAGE_ID,processor_.GetKeySize(),this->leaf_max_size_);
  //插入数据
  root_page->Insert(key,value,processor_);
  buffer_pool_manager_->UnpinPage(root_page_id_,true);
  //更新index_roots_page
  this->UpdateRootPageId(1);
}

/*
 * Insert constant key & value pair into leaf page
 * User needs to first find the right leaf page as insertion target, then look
 * through leaf page to see whether insert key exist or not. If exist, return
 * immediately, otherwise insert entry. Remember to deal with split if necessary.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
bool BPlusTree::InsertIntoLeaf(GenericKey *key, const RowId &value, Transaction *transaction) {
  Page* leaf_page=this->FindLeafPage(key);
  BPlusTreeLeafPage* leaf_node=reinterpret_cast<BPlusTreeLeafPage*>(leaf_page->GetData());
  ASSERT(leaf_page!= nullptr,"wrong");
  RowId temp;//为了用Lookup
  if(leaf_node->Lookup(key,temp,processor_)){
    buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(),false);
    return false;//已经有这个key了
  }
  if(leaf_node->GetSize()<leaf_node->GetMaxSize())//没满
    leaf_node->Insert(key,value,processor_);
  else{//满了,需要分裂
    BPlusTreeLeafPage* new_leaf_node= Split(leaf_node,transaction);
    if(processor_.CompareKeys(new_leaf_node->KeyAt(0),key)>0){//插在前面
      leaf_node->Insert(key,value,processor_);
    }else {  // 插在后面
      new_leaf_node->Insert(key, value, processor_);
    }
    //插入父节点
    this->InsertIntoParent(leaf_node,new_leaf_node->KeyAt(0),new_leaf_node,transaction);
    buffer_pool_manager_->UnpinPage(new_leaf_node->GetPageId(),true);
  }
  buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(),true);
  return true;
}

/*
 * Split input page and return newly created page.
 * Using template N to represent either internal page or leaf page.
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then move half
 * of key & value pairs from input page to newly created page
 */
BPlusTreeInternalPage *BPlusTree::Split(InternalPage *node, Transaction *transaction) {
  page_id_t new_page_id;
  Page* new_page=buffer_pool_manager_->NewPage(new_page_id);
  ASSERT(new_page!= nullptr,"out of memory");
  BPlusTreeInternalPage* new_node=reinterpret_cast<BPlusTreeInternalPage*>(new_page->GetData());
  new_node->Init(new_page_id,node->GetParentPageId(),processor_.GetKeySize(),this->internal_max_size_);
  node->MoveHalfTo(new_node,buffer_pool_manager_);
  return new_node;//外面需要Unpin
}

BPlusTreeLeafPage *BPlusTree::Split(LeafPage *node, Transaction *transaction) {
  page_id_t new_page_id;
  Page* new_page=buffer_pool_manager_->NewPage(new_page_id);
  ASSERT(new_page!= nullptr,"out of memory");
  BPlusTreeLeafPage* new_node=reinterpret_cast<BPlusTreeLeafPage*>(new_page->GetData());
  new_node->Init(new_page_id,node->GetParentPageId(),processor_.GetKeySize(),this->leaf_max_size_);
  node->MoveHalfTo(new_node);
  //别忘了设置next_page
  new_node->SetNextPageId(node->GetNextPageId());
  node->SetNextPageId(new_node->GetPageId());
  return new_node;
}

/*
 * Insert key & value pair into internal page after split
 * @param   old_node      input page from split() method
 * @param   key
 * @param   new_node      returned page from split() method
 * User needs to first find the parent page of old_node, parent node must be
 * adjusted to take info of new_node into account. Remember to deal with split
 * recursively if necessary.
 */
void BPlusTree::InsertIntoParent(BPlusTreePage *old_node, GenericKey *key, BPlusTreePage *new_node,
                                 Transaction *transaction) {
  page_id_t parent_page_id=old_node->GetParentPageId();//父节点的page_id
  Page* parent_page;
  BPlusTreeInternalPage* parent_node;
  if(old_node->IsRootPage()){//如果old_node是根节点
    //新建一个根节点
    parent_page=buffer_pool_manager_->NewPage(parent_page_id);
    this->root_page_id_=parent_page_id;//记得更改
    parent_node=reinterpret_cast<BPlusTreeInternalPage*>(parent_page->GetData());
    parent_node->Init(parent_page_id,INVALID_PAGE_ID,processor_.GetKeySize(),this->internal_max_size_);
    //插入数据
    parent_node->PopulateNewRoot(old_node->GetPageId(),key,new_node->GetPageId());
    //更新index_roots_page
    this->UpdateRootPageId(0);
    //设置子节点
    old_node->SetParentPageId(parent_page_id);
    new_node->SetParentPageId(parent_page_id);
  }else{//不是根节点
    parent_page=buffer_pool_manager_->FetchPage(parent_page_id);
    parent_node=reinterpret_cast<BPlusTreeInternalPage*>(parent_page->GetData());
    if(parent_node->GetSize()<parent_node->GetMaxSize()){//还没满，不用分裂
      parent_node->InsertNodeAfter(old_node->GetPageId(),key,new_node->GetPageId());
      new_node->SetParentPageId(parent_page_id);
    }else{//需要分裂
      //分裂父节点
      BPlusTreeInternalPage* new_parent_node=this->Split(parent_node,transaction);
      //插入新的节点
      if(processor_.CompareKeys(new_parent_node->KeyAt(0),key)>0){//插在前面
        parent_node->InsertNodeAfter(old_node->GetPageId(),key,new_node->GetPageId());
//        new_node->SetParentPageId(parent_page_id);//这行没必要，和Split部分重复了
      }else{//插在后面
        new_parent_node->InsertNodeAfter(old_node->GetPageId(),key,new_node->GetPageId());
        old_node->SetParentPageId(new_parent_node->GetPageId());
        new_node->SetParentPageId(new_parent_node->GetPageId());
      }
      //插入父的父节点
      this->InsertIntoParent(parent_node,new_parent_node->KeyAt(0),new_parent_node);
      //Unpin
      this->buffer_pool_manager_->UnpinPage(new_parent_node->GetPageId(),true);
    }
  }
  buffer_pool_manager_->UnpinPage(parent_page_id,true);
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Delete key & value pair associated with input key
 * If current tree is empty, return immediately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.
 */
void BPlusTree::Remove(const GenericKey *key, Transaction *transaction) {
  if(IsEmpty()) return;//empty
  Page* leaf_page=this->FindLeafPage(key);
  BPlusTreeLeafPage* leaf_node=reinterpret_cast<BPlusTreeLeafPage*>(leaf_page->GetData());
  leaf_node->RemoveAndDeleteRecord(key,processor_);
  bool res=false;
  if(leaf_node->GetSize()<leaf_node->GetMinSize()){
    res=this->CoalesceOrRedistribute(leaf_node,transaction);
  }
  if(!res) buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(),true);
}

/* todo
 * User needs to first find the sibling of input page. If sibling's size + input
 * page's size > page's max size, then redistribute. Otherwise, merge.
 * Using template N to represent either internal page or leaf page.
 * @return: true means target leaf page should be deleted, false means no
 * deletion happens
 */
template <typename N>
bool BPlusTree::CoalesceOrRedistribute(N *&node, Transaction *transaction) {
  ASSERT(node!= nullptr,"wrong");
  ASSERT(node->GetSize()<node->GetMinSize(),"wrong");
  //根节点
  if(node->IsRootPage()){
    return AdjustRoot(node);
  }
  //不是根节点的情况
  bool res;
  //先获取父节点
  Page* parent_page=buffer_pool_manager_->FetchPage(node->GetParentPageId());
  BPlusTreeInternalPage* parent_node=reinterpret_cast<BPlusTreeInternalPage*>(parent_page->GetData());
  //然后获取兄弟节点
  Page* sibling_page;
  int index=parent_node->ValueIndex(node->GetPageId());
  if(index ==0 ) sibling_page=buffer_pool_manager_->FetchPage(parent_node->ValueAt(index+1));
  else sibling_page=buffer_pool_manager_->FetchPage(parent_node->ValueAt(index-1));
  N* sibling_node=reinterpret_cast<N*>(sibling_page->GetData());
  if(sibling_node->GetSize()+node->GetSize()<sibling_node->GetMaxSize()){//合并
    if(index==0){//把sibling移到node
      this->Coalesce(node,sibling_node,parent_node,1,transaction);
      res=false;//node不会被删
    }else{//node移动sibling
      this->Coalesce(sibling_node,node,parent_node,index,transaction);
      res=true;//node会被删
    }
  }else{//借节点
    this->Redistribute(sibling_node,node,index);
    res=false;
  }
  buffer_pool_manager_->UnpinPage(sibling_page->GetPageId(),true);
  buffer_pool_manager_->UnpinPage(parent_page->GetPageId(),true);
//  if(res) buffer_pool_manager_->DeletePage(parent_page->GetPageId());//不需要
  return res;
}

/*
 * Move all the key & value pairs from one page to its sibling page, and notify
 * buffer pool manager to delete this page. Parent page must be adjusted to
 * take info of deletion into account. Remember to deal with coalesce or
 * redistribute recursively if necessary.
 * Using template N to represent either internal page or leaf page.
 * @param   neighbor_node      sibling page of input "node"
 * @param   node               input from method coalesceOrRedistribute()
 * @param   parent             parent page of input "node"
 * @return  true means parent node should be deleted, false means no deletion happened
 */
bool BPlusTree::Coalesce(LeafPage *&neighbor_node, LeafPage *&node, InternalPage *&parent, int index,
                         Transaction *transaction) {
  ASSERT(neighbor_node->GetSize()+node->GetSize()<=neighbor_node->GetMaxSize(),"wrong");
  node->MoveAllTo(neighbor_node);
  //调整parant
  parent->Remove(index);
  //删除node，不需要设置为为dirty
  buffer_pool_manager_->UnpinPage(node->GetPageId(),false);
  buffer_pool_manager_->DeletePage(node->GetPageId());
  //parent是根节点
  if(parent->IsRootPage()) return parent->GetSize()>1?false:true;
  else if(parent->GetSize()<parent->GetMinSize()){
    return this->CoalesceOrRedistribute<BPlusTreeInternalPage>(parent,transaction);
  }
  return false;
}

bool BPlusTree::Coalesce(InternalPage *&neighbor_node, InternalPage *&node, InternalPage *&parent, int index,
                         Transaction *transaction) {
  ASSERT(neighbor_node->GetSize()+node->GetSize()<=neighbor_node->GetMaxSize(),"wrong");
  node->MoveAllTo(neighbor_node,parent->KeyAt(index),buffer_pool_manager_);
  //调整parent
  parent->Remove(index);
  //删除node，不需要设置为为dirty
  buffer_pool_manager_->UnpinPage(node->GetPageId(),false);
  buffer_pool_manager_->DeletePage(node->GetPageId());
  if(parent->IsRootPage()&&parent->GetSize()<parent->GetMinSize())
    return this->CoalesceOrRedistribute<BPlusTreeInternalPage>(parent,transaction);
  return false;
}

/*
 * Redistribute key & value pairs from one page to its sibling page. If index ==
 * 0, move sibling page's first key & value pair into end of input "node",
 * otherwise move sibling page's last key & value pair into head of input
 * "node".
 * Using template N to represent either internal page or leaf page.
 * @param   neighbor_node      sibling page of input "node"
 * @param   node               input from method coalesceOrRedistribute()
 */
void BPlusTree::Redistribute(LeafPage *neighbor_node, LeafPage *node, int index) {
  Page* parent_page=buffer_pool_manager_->FetchPage(node->GetParentPageId());
  BPlusTreeInternalPage* parent_node=reinterpret_cast<BPlusTreeInternalPage*>(parent_page->GetData());
  if(index==0){
    neighbor_node->MoveFirstToEndOf(node);
    int p_index=parent_node->ValueIndex(neighbor_node->GetPageId());
    parent_node->SetKeyAt(p_index,neighbor_node->KeyAt(0));//value不用改;
  }
  else{
    neighbor_node->MoveLastToFrontOf(node);
    int p_index=parent_node->ValueAt(node->GetPageId());
    parent_node->SetKeyAt(p_index,node->KeyAt(0));
  }
  buffer_pool_manager_->UnpinPage(parent_page->GetPageId(),true);
}
void BPlusTree::Redistribute(InternalPage *neighbor_node, InternalPage *node, int index) {
  Page* parent_page=buffer_pool_manager_->FetchPage(node->GetParentPageId());
  BPlusTreeInternalPage* parent_node=reinterpret_cast<BPlusTreeInternalPage*>(parent_page->GetData());
  if(index==0){
    int p_index=parent_node->ValueIndex(neighbor_node->GetPageId());
    neighbor_node->MoveFirstToEndOf(node,parent_node->KeyAt(p_index),buffer_pool_manager_);
    parent_node->SetKeyAt(p_index,neighbor_node->KeyAt(0));//value不用改;
  }
  else{
    int p_index=parent_node->ValueAt(node->GetPageId());
    neighbor_node->MoveLastToFrontOf(node,parent_node->KeyAt(p_index),buffer_pool_manager_);
    parent_node->SetKeyAt(p_index,node->KeyAt(0));
  }
  buffer_pool_manager_->UnpinPage(parent_page->GetPageId(),true);
}
/*
 * Update root page if necessary
 * NOTE: size of root page can be less than min size and this method is only
 * called within coalesceOrRedistribute() method
 * case 1: when you delete the last element in root page, but root page still
 * has one last child
 * case 2: when you delete the last element in whole b+ tree
 * @return : true means root page should be deleted, false means no deletion
 * happened
 */
bool BPlusTree::AdjustRoot(BPlusTreePage *old_root_node) {
//  old_root_node->IncreaseSize(-1);
//  if (old_root_node->GetSize() == 0) {
  if(old_root_node->IsLeafPage()&&old_root_node->GetSize()==0) {//case2
    this->root_page_id_ = INVALID_PAGE_ID;
    UpdateRootPageId(0);  // 更新
    buffer_pool_manager_->UnpinPage(old_root_node->GetPageId(),false);
    buffer_pool_manager_->DeletePage(old_root_node->GetPageId());
    return true;
  }else if(old_root_node->GetSize()==1&&!old_root_node->IsLeafPage()) {//case1
    BPlusTreeInternalPage* old_root_internal_node=reinterpret_cast<BPlusTreeInternalPage*>(old_root_node);
    this->root_page_id_=old_root_internal_node->RemoveAndReturnOnlyChild();
    this->UpdateRootPageId(0);
    //得到新的root
    Page* new_root_page=buffer_pool_manager_->FetchPage(this->root_page_id_);
    BPlusTreePage* new_root_node=reinterpret_cast<BPlusTreePage*>(new_root_page->GetData());
    new_root_node->SetParentPageId(INVALID_PAGE_ID);
    buffer_pool_manager_->UnpinPage(this->root_page_id_,true);
    // 因为要删除，没必要设置为dirty
    buffer_pool_manager_->UnpinPage(old_root_node->GetPageId(),false);
    buffer_pool_manager_->DeletePage(old_root_node->GetPageId());
    return true;
  }
  return false;
}

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the left most leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
IndexIterator BPlusTree::Begin() {
  GenericKey* key;
  Page* left_page=this->FindLeafPage(key,INVALID_PAGE_ID,true);
  return IndexIterator(left_page->GetPageId(),buffer_pool_manager_,0);
}

/*
 * Input parameter is low-key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
IndexIterator BPlusTree::Begin(const GenericKey *key) {
  Page* page=this->FindLeafPage(key,0, false);
  BPlusTreeLeafPage* node=reinterpret_cast<BPlusTreeLeafPage*>(page);
  int index=node->KeyIndex(key,processor_);
  return IndexIterator(page->GetPageId(),buffer_pool_manager_,index);
}

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
IndexIterator BPlusTree::End() {
  return IndexIterator(INVALID_PAGE_ID,buffer_pool_manager_);
}

/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/
/*
 * Find leaf page containing particular key, if leftMost flag == true, find
 * the left most leaf page
 * Note: the leaf page is pinned, you need to unpin it after use.
 */
Page *BPlusTree::FindLeafPage(const GenericKey *key, page_id_t page_id, bool leftMost) {
  if(this->IsEmpty()) return nullptr;//空树
  Page* page=this->buffer_pool_manager_->FetchPage(this->root_page_id_);//根
  auto bp_page=reinterpret_cast<BPlusTreePage*>(page->GetData());
  while(!bp_page->IsLeafPage()){
    auto internal_page=reinterpret_cast<BPlusTreeInternalPage*>(bp_page);
    page_id_t page_id;//next_page_id
    if(leftMost) page_id=internal_page->ValueAt(0);
    else page_id=internal_page->Lookup(key,this->processor_);
    this->buffer_pool_manager_->UnpinPage(page->GetPageId(),false);
    page=this->buffer_pool_manager_->FetchPage(page_id);
    bp_page=reinterpret_cast<BPlusTreePage*>(page->GetData());
  }
  return page;//函数外记得Unpin
}

/*
 * Update/Insert root page id in header page(where page_id = 0, header_page is
 * defined under include/page/header_page.h)
 * Call this method everytime root page id is changed.
 * @parameter: insert_record      default value is false. When set to true,
 * insert a record <index_name, current_page_id> into header page instead of
 * updating it.
 */
void BPlusTree::UpdateRootPageId(int insert_record) {
  Page* index_page=buffer_pool_manager_->FetchPage(INDEX_ROOTS_PAGE_ID);
  IndexRootsPage* index_roots_page=reinterpret_cast<IndexRootsPage*>(index_page->GetData());
  if(insert_record==0) index_roots_page->Update(index_id_,root_page_id_);
  else index_roots_page->Insert(this->index_id_,this->root_page_id_);
  buffer_pool_manager_->UnpinPage(INDEX_ROOTS_PAGE_ID,true);
}

/**
 * This method is used for debug only, You don't need to modify
 */
void BPlusTree::ToGraph(BPlusTreePage *page, BufferPoolManager *bpm, std::ofstream &out) const {
  std::string leaf_prefix("LEAF_");
  std::string internal_prefix("INT_");
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<LeafPage *>(page);
    // Print node name
    out << leaf_prefix << leaf->GetPageId();
    // Print node properties
    out << "[shape=plain color=green ";
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">P=" << leaf->GetPageId()
        << ",Parent=" << leaf->GetParentPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">"
        << "max_size=" << leaf->GetMaxSize() << ",min_size=" << leaf->GetMinSize() << ",size=" << leaf->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < leaf->GetSize(); i++) {
      out << "<TD>" << leaf->KeyAt(i) << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Leaf node link if there is a next page
    if (leaf->GetNextPageId() != INVALID_PAGE_ID) {
      out << leaf_prefix << leaf->GetPageId() << " -> " << leaf_prefix << leaf->GetNextPageId() << ";\n";
      out << "{rank=same " << leaf_prefix << leaf->GetPageId() << " " << leaf_prefix << leaf->GetNextPageId() << "};\n";
    }

    // Print parent links if there is a parent
    if (leaf->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << leaf->GetParentPageId() << ":p" << leaf->GetPageId() << " -> " << leaf_prefix
          << leaf->GetPageId() << ";\n";
    }
  } else {
    auto *inner = reinterpret_cast<InternalPage *>(page);
    // Print node name
    out << internal_prefix << inner->GetPageId();
    // Print node properties
    out << "[shape=plain color=pink ";  // why not?
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">P=" << inner->GetPageId()
        << ",Parent=" << inner->GetParentPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">"
        << "max_size=" << inner->GetMaxSize() << ",min_size=" << inner->GetMinSize() << ",size=" << inner->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < inner->GetSize(); i++) {
      out << "<TD PORT=\"p" << inner->ValueAt(i) << "\">";
      if (i > 0) {
        out << inner->KeyAt(i);
      } else {
        out << " ";
      }
      out << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Parent link
    if (inner->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << inner->GetParentPageId() << ":p" << inner->GetPageId() << " -> " << internal_prefix
          << inner->GetPageId() << ";\n";
    }
    // Print leaves
    for (int i = 0; i < inner->GetSize(); i++) {
      auto child_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i))->GetData());
      ToGraph(child_page, bpm, out);
      if (i > 0) {
        auto sibling_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i - 1))->GetData());
        if (!sibling_page->IsLeafPage() && !child_page->IsLeafPage()) {
          out << "{rank=same " << internal_prefix << sibling_page->GetPageId() << " " << internal_prefix
              << child_page->GetPageId() << "};\n";
        }
        bpm->UnpinPage(sibling_page->GetPageId(), false);
      }
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

/**
 * This function is for debug only, you don't need to modify
 */
void BPlusTree::ToString(BPlusTreePage *page, BufferPoolManager *bpm) const {
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<LeafPage *>(page);
    std::cout << "Leaf Page: " << leaf->GetPageId() << " parent: " << leaf->GetParentPageId()
              << " next: " << leaf->GetNextPageId() << std::endl;
    for (int i = 0; i < leaf->GetSize(); i++) {
      std::cout << leaf->KeyAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
  } else {
    auto *internal = reinterpret_cast<InternalPage *>(page);
    std::cout << "Internal Page: " << internal->GetPageId() << " parent: " << internal->GetParentPageId() << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      std::cout << internal->KeyAt(i) << ": " << internal->ValueAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      ToString(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(internal->ValueAt(i))->GetData()), bpm);
      bpm->UnpinPage(internal->ValueAt(i), false);
    }
  }
}

bool BPlusTree::Check() {
  bool all_unpinned = buffer_pool_manager_->CheckAllUnpinned();
  if (!all_unpinned) {
    LOG(ERROR) << "problem in page unpin" << endl;
  }
  return all_unpinned;
}