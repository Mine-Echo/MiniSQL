#include "catalog/catalog.h"

void CatalogMeta::SerializeTo(char *buf) const {
    ASSERT(GetSerializedSize() <= PAGE_SIZE, "Failed to serialize catalog metadata to disk.");
    MACH_WRITE_UINT32(buf, CATALOG_METADATA_MAGIC_NUM);
    buf += 4;
    MACH_WRITE_UINT32(buf, table_meta_pages_.size());
    buf += 4;
    MACH_WRITE_UINT32(buf, index_meta_pages_.size());
    buf += 4;
    for (auto iter : table_meta_pages_) {
        MACH_WRITE_TO(table_id_t, buf, iter.first);
        buf += 4;
        MACH_WRITE_TO(page_id_t, buf, iter.second);
        buf += 4;
    }
    for (auto iter : index_meta_pages_) {
        MACH_WRITE_TO(index_id_t, buf, iter.first);
        buf += 4;
        MACH_WRITE_TO(page_id_t, buf, iter.second);
        buf += 4;
    }
}

CatalogMeta *CatalogMeta::DeserializeFrom(char *buf) {
    // check valid
    uint32_t magic_num = MACH_READ_UINT32(buf);
    buf += 4;
    ASSERT(magic_num == CATALOG_METADATA_MAGIC_NUM, "Failed to deserialize catalog metadata from disk.");
    // get table and index nums
    uint32_t table_nums = MACH_READ_UINT32(buf);
    buf += 4;
    uint32_t index_nums = MACH_READ_UINT32(buf);
    buf += 4;
    // create metadata and read value
    CatalogMeta *meta = new CatalogMeta();
    for (uint32_t i = 0; i < table_nums; i++) {
        auto table_id = MACH_READ_FROM(table_id_t, buf);
        buf += 4;
        auto table_heap_page_id = MACH_READ_FROM(page_id_t, buf);
        buf += 4;
        meta->table_meta_pages_.emplace(table_id, table_heap_page_id);
    }
    for (uint32_t i = 0; i < index_nums; i++) {
        auto index_id = MACH_READ_FROM(index_id_t, buf);
        buf += 4;
        auto index_page_id = MACH_READ_FROM(page_id_t, buf);
        buf += 4;
        meta->index_meta_pages_.emplace(index_id, index_page_id);
    }
    return meta;
}

/**
 * TODO: Student Implement
 */
uint32_t CatalogMeta::GetSerializedSize() const {
//  ASSERT(false, "Not Implemented yet");
  return 3*4+table_meta_pages_.size()*8+index_meta_pages_.size()*8;
}

CatalogMeta::CatalogMeta() {}

/**
 * TODO: Student Implement
 */
CatalogManager::CatalogManager(BufferPoolManager *buffer_pool_manager, LockManager *lock_manager,
                               LogManager *log_manager, bool init)
    : buffer_pool_manager_(buffer_pool_manager), lock_manager_(lock_manager), log_manager_(log_manager) {
//  ASSERT(false, "Not Implemented yet");
  if(init){
    this->catalog_meta_=CatalogMeta::NewInstance();
    this->next_index_id_=catalog_meta_->GetNextIndexId();
    this->next_table_id_=catalog_meta_->GetNextTableId();
  }else{//从文件中初始化信息
    Page* meta_page=this->buffer_pool_manager_->FetchPage(CATALOG_META_PAGE_ID);
    this->catalog_meta_=CatalogMeta::DeserializeFrom(meta_page->GetData());
    this->next_table_id_=catalog_meta_->GetNextTableId();
    this->next_index_id_=catalog_meta_->GetNextIndexId();
    for(auto it:catalog_meta_->table_meta_pages_){
      ASSERT(LoadTable(it.first,it.second)==DB_SUCCESS,"wrong");
    }
    for(auto it:catalog_meta_->index_meta_pages_){
      ASSERT(LoadIndex(it.first,it.second)==DB_SUCCESS,"wrong");
    }
  }
}

CatalogManager::~CatalogManager() {
  FlushCatalogMetaPage();
  delete catalog_meta_;
  for (auto iter : tables_) {
    delete iter.second;
  }
  for (auto iter : indexes_) {
    delete iter.second;
  }
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::CreateTable(const string &table_name, TableSchema *schema,
                                    Transaction *txn, TableInfo *&table_info) {
  if(table_names_.find(table_name)!=table_names_.end())
    return DB_TABLE_ALREADY_EXIST;
  //table_heap
  TableHeap* table_heap=TableHeap::Create(buffer_pool_manager_,schema, nullptr,log_manager_,lock_manager_);
  TableMetadata* meta_data=TableMetadata::Create(next_table_id_++,table_name,table_heap->GetFirstPageId(),schema);
  table_info=TableInfo::Create();
  table_info->Init(meta_data,table_heap);
  //table_name&tables
  table_names_[table_name]=next_table_id_-1;
  tables_[next_table_id_-1]=table_info;
  //找个page写table元数据
  page_id_t page_id;
  Page* page=buffer_pool_manager_->NewPage(page_id);
  meta_data->SerializeTo(page->GetData());
  buffer_pool_manager_->UnpinPage(page_id,true);
  //更新catalog metadata
  this->catalog_meta_->table_meta_pages_[next_table_id_-1]=page_id;
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::GetTable(const string &table_name, TableInfo *&table_info) {
  auto it=table_names_.find(table_name);
  if(it==table_names_.end()) return DB_TABLE_NOT_EXIST;
  GetTable(it->second,table_info);
//  table_info=tables_[it->second];
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::GetTables(vector<TableInfo *> &tables) const {
  for(auto it:tables_)
    tables.push_back(it.second);
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::CreateIndex(const std::string &table_name, const string &index_name,
                                    const std::vector<std::string> &index_keys, Transaction *txn,
                                    IndexInfo *&index_info, const string &index_type) {
  // ASSERT(false, "Not Implemented yet");
  dberr_t dberr= GetIndex(table_name,index_name,index_info);
  if(dberr!=DB_INDEX_NOT_FOUND) return dberr==DB_SUCCESS?DB_INDEX_ALREADY_EXIST:dberr;
  index_id_t index_id=this->next_index_id_++;
  table_id_t table_id=table_names_[table_name];
  TableInfo* table_info=tables_[table_id];
  uint32_t col_index;
  std::vector<uint32_t> key_map;
  for(auto key:index_keys){
    if(table_info->GetSchema()->GetColumnIndex(key,col_index)==DB_COLUMN_NAME_NOT_EXIST)
      return DB_COLUMN_NAME_NOT_EXIST;
    key_map.push_back(col_index);
  }
  IndexMetadata* meta_data=IndexMetadata::Create(index_id,index_name,table_id,key_map);
  index_info=IndexInfo::Create();
  index_info->Init(meta_data,table_info,buffer_pool_manager_);
  //将table中原有的数据插入索引中
  auto itr=table_info->GetTableHeap()->Begin(txn);
  vector<uint32_t> column_ids;
  vector<Column *> columns = index_info->GetIndexKeySchema()->GetColumns();
  for (auto column : columns) {
    uint32_t column_id;
    if (table_info->GetSchema()->GetColumnIndex(column->GetName(), column_id) == DB_SUCCESS)
      column_ids.push_back(column_id);
  }
  for(;itr!=table_info->GetTableHeap()->End();itr++){
    Row tmp=*itr;
    vector<Field> fields;
    for (auto column_id : column_ids) fields.push_back(*tmp.GetField(column_id));
    Row index_row(fields);
    index_info->GetIndex()->InsertEntry(index_row,tmp.GetRowId(),txn);
  }
  //找个page写元数据
  page_id_t page_id;
  Page* page=buffer_pool_manager_->NewPage(page_id);
  meta_data->SerializeTo(page->GetData());
  buffer_pool_manager_->UnpinPage(page_id,true);
  //更新table_names&indexes
  index_names_[table_name][index_name]=index_id;
  indexes_[index_id]=index_info;
  //更新catalog_meta_data
  catalog_meta_->index_meta_pages_[index_id]=page_id;
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::GetIndex(const std::string &table_name, const std::string &index_name,
                                 IndexInfo *&index_info) const {
  auto it1=table_names_.find(table_name);
  if(it1==table_names_.end()) return DB_TABLE_NOT_EXIST;
  auto it2=index_names_.find(table_name);
  if(it2==index_names_.end()) return DB_INDEX_NOT_FOUND;
  auto it3=it2->second.find(index_name);
  if(it3==it2->second.end()) return DB_INDEX_NOT_FOUND;
  index_info=indexes_.at(it3->second);
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::GetTableIndexes(const std::string &table_name, std::vector<IndexInfo *> &indexes) const {
  auto it1=table_names_.find(table_name);
  if(it1==table_names_.end()) return DB_TABLE_NOT_EXIST;
  auto it2=index_names_.find(table_name);
  if(it2==index_names_.end()) return DB_INDEX_NOT_FOUND;
  for(auto it3:(it2->second)){
    indexes.push_back(indexes_.at(it3.second));
  }
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::DropTable(const string &table_name) {
  // ASSERT(false, "Not Implemented yet");
  auto it=table_names_.find(table_name);
  if(it==table_names_.end()) return DB_TABLE_NOT_EXIST;
  //先删索引
  auto index_it=index_names_.find(table_name);
  if(index_it!=index_names_.end()){
    vector<IndexInfo *>indexes;
    GetTableIndexes(table_name,indexes);
    for(int i=0;i<indexes.size();i++)
      DropIndex(table_name,indexes[i]->GetIndexName());
  }
  TableInfo* table_info=tables_[it->second];
  TableHeap* table_heap=table_info->GetTableHeap();
  table_heap->DeleteTable(table_heap->GetFirstPageId());
  //删除元数据所在的磁盘页
  if(!buffer_pool_manager_->DeletePage(catalog_meta_->GetTableMetaPages()->at(it->second)))
    return DB_FAILED;
  //从catalog_meta_data中删除
  this->catalog_meta_->table_meta_pages_.erase(it->second);
  //tables和table_name中删除
  tables_.erase(it->second);
  table_names_.erase(table_name);
  delete table_info;
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::DropIndex(const string &table_name, const string &index_name) {
  // ASSERT(false, "Not Implemented yet");
  auto it1=index_names_.find(table_name);
  if(it1==index_names_.end()) return DB_TABLE_NOT_EXIST;
  auto it2=it1->second.find(index_name);
  if(it2==it1->second.end()) return DB_INDEX_NOT_FOUND;
  IndexInfo* index_info=indexes_[it2->second];
  //删除元数据所在的磁盘页
  if(!buffer_pool_manager_->DeletePage(catalog_meta_->GetIndexMetaPages()->at(it2->second)))
    return DB_FAILED;
  //catalog
  this->catalog_meta_->index_meta_pages_.erase(it2->second);
  //indexs&index_name
  this->indexes_.erase(it2->second);
  this->index_names_[table_name].erase(index_name);
  index_info->GetIndex()->Destroy();
  delete index_info;
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::FlushCatalogMetaPage() const {
  Page* page=buffer_pool_manager_->FetchPage(CATALOG_META_PAGE_ID);
  this->catalog_meta_->SerializeTo(page->GetData());
  buffer_pool_manager_->UnpinPage(CATALOG_META_PAGE_ID,true);
  buffer_pool_manager_->FlushPage(CATALOG_META_PAGE_ID);
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::LoadTable(const table_id_t table_id, const page_id_t page_id) {
  Page* page=buffer_pool_manager_->FetchPage(page_id);
  //meta_data
  TableMetadata* meta_data;
  TableMetadata::DeserializeFrom(page->GetData(),meta_data);
  //table_heap
  TableHeap* table_heap=TableHeap::Create(buffer_pool_manager_,meta_data->GetFirstPageId(),meta_data->GetSchema(),log_manager_,lock_manager_);
  //table_info
  TableInfo* table_info=TableInfo::Create();
  table_info->Init(meta_data,table_heap);
  //添加到table和table_name里面
  ASSERT(table_id==meta_data->GetTableId(),"wrong");
  this->table_names_[meta_data->GetTableName()]=table_id;
  this->tables_[table_id]=table_info;
  //Unpin
  buffer_pool_manager_->UnpinPage(page_id,false);
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::LoadIndex(const index_id_t index_id, const page_id_t page_id) {
  // ASSERT(false, "Not Implemented yet");
  Page* page=buffer_pool_manager_->FetchPage(page_id);
  //meta_data
  IndexMetadata* meta_data;
  IndexMetadata::DeserializeFrom(page->GetData(),meta_data);
  ASSERT(index_id==meta_data->GetIndexId(),"wrong");
  //index_info
  IndexInfo* index_info=IndexInfo::Create();
  index_info->Init(meta_data,tables_[meta_data->GetTableId()],buffer_pool_manager_);
  //table_name&index_name
  std::string index_name=meta_data->GetIndexName();
  std::string table_name=tables_[meta_data->GetTableId()]->GetTableName();
  this->index_names_[table_name][index_name]=index_id;
  this->indexes_[index_id]=index_info;
  buffer_pool_manager_->UnpinPage(page_id,false);
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::GetTable(const table_id_t table_id, TableInfo *&table_info) {
  // ASSERT(false, "Not Implemented yet");
  auto it=tables_.find(table_id);
  if(it==tables_.end()) return DB_TABLE_NOT_EXIST;
  table_info=tables_[table_id];
  return DB_SUCCESS;
}