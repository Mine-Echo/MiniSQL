//
// Created by njz on 2023/1/30.
//

#include "executor/executors/update_executor.h"
#include "planner/expressions/constant_value_expression.h"
UpdateExecutor::UpdateExecutor(ExecuteContext *exec_ctx, const UpdatePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

/**
* TODO: Student Implement
*/
void UpdateExecutor::Init() {
  child_executor_->Init();
}

bool UpdateExecutor::Next([[maybe_unused]] Row *row, RowId *rid) {
//  Row update_row{};
//  RowId update_rowid{};
//  if(child_executor_->Next(&update_row, &update_rowid)){
//    Row new_row = GenerateUpdatedTuple(update_row);
//    new_row.SetRowId(update_rowid);
//
//    //find the table info
//    TableInfo *table_info = nullptr;
//    if(exec_ctx_->GetCatalog()->GetTable(plan_->table_name_, table_info) != DB_SUCCESS){
//      return false;
//    }
//    //find if has the duplicate key
////    if(plan_->GetUpdateAttr().
//    vector<IndexInfo *>indexes;
//    if(exec_ctx_->GetCatalog()->GetTableIndexes(plan_->GetTableName(),indexes) == DB_SUCCESS){
//      //traverse the indexes
//      for(auto index: indexes){
//        vector<uint32_t>column_ids;
//        vector<Column *>index_columns = index->GetIndexKeySchema()->GetColumns();
//        //init the column ids in table
//        for(auto index_column: index_columns){
//          uint32_t index_column_id;
//          if (table_info->GetSchema()->GetColumnIndex(index_column->GetName(), index_column_id) == DB_SUCCESS){
//            column_ids.emplace_back(index_column_id);
//          }
//        }
//        //init the fields for index
//        vector<Field> fields;
//        for(auto id: column_ids){
//          fields.emplace_back(*new_row.GetField(id));
//        }
//        Row index_row(fields);
//        vector<RowId> temp_result;
//        //if find the duplicate key
//        if(index->GetIndex()->ScanKey(index_row, temp_result, exec_ctx_->GetTransaction(), "=") == DB_SUCCESS){
//          cout << "Duplicate entry for key '" << index->GetIndexName() <<  "'." << endl;
//          return false;
//        }
//      }
//    }
//
//    //if no duplicate key, we update the index
//    if(!indexes.empty()){
//      for(auto index: indexes){
//        vector<uint32_t>column_ids;
//        vector<Column *>index_columns = index->GetIndexKeySchema()->GetColumns();
//        //init the column id in the original table
//        for(auto index_column: index_columns){
//          uint32_t index_column_id;
//          if (table_info->GetSchema()->GetColumnIndex(index_column->GetName(), index_column_id) == DB_SUCCESS){
//            column_ids.emplace_back(index_column_id);
//          }
//        }
//        //init the fields for index
//        vector<Field> fields;
//        for(auto id: column_ids){
//          fields.emplace_back(*new_row.GetField(id));
//        }
//        Row index_row(fields);
//        //if find the duplicate key
//        index->GetIndex()->RemoveEntry(index_row, new_row.GetRowId(), exec_ctx_->GetTransaction());
//        index->GetIndex()->InsertEntry(index_row, new_row.GetRowId(), exec_ctx_->GetTransaction());
//      }
//    }
//    table_info->GetTableHeap()->UpdateTuple(new_row, new_row.GetRowId(), exec_ctx_->GetTransaction());
//
//    return true;
//  }
//  else{
//    return false;
//  }
  TableInfo *tableInfo = nullptr;
  exec_ctx_->GetCatalog()->GetTable(plan_->GetTableName(), tableInfo);
  TableHeap *tableHeap = tableInfo->GetTableHeap();
  TableMetadata *tableMetadata = tableInfo->GetTableMetaData();
  Row old_row, new_row;
  RowId old_rowid;
  if(child_executor_->Next(&old_row, &old_rowid))
  {
    std::vector<IndexInfo *> indexes;
    exec_ctx_->GetCatalog()->GetTableIndexes(plan_->GetTableName(), indexes);
    new_row = GenerateUpdatedTuple(old_row);
    if(!indexes.empty()) {
      // primary key check
      vector<string> primary_key;  // = tableMetadata->primary_key;
      IndexInfo *index_info;
      exec_ctx_->GetCatalog()->GetIndex(tableInfo->GetTableName(), tableInfo->GetTableName() + "_primary_index",
                                        index_info);
      auto columns = index_info->GetIndexKeySchema()->GetColumns();
      for (auto column : columns) primary_key.push_back(column->GetName());

      if (primary_key.size() != 0) {
        uint32_t column_index;
        vector<Field> fields;
        Row temp_row;
        for (auto key_name : primary_key) {
          tableInfo->GetSchema()->GetColumnIndex(key_name, column_index);
          if (new_row.GetField(column_index)->IsNull()) {
            cout << "Error: Primary key columns can't be null" << endl;
            return false;
          }
          fields.push_back(*(new_row.GetField(column_index)));
        }
        temp_row = Row(fields);
        auto it = indexes.begin();
        for (; it != indexes.end() && (*it)->GetIndexName() != tableInfo->GetTableName() + "_primary_index"; it++)
          ;
        BPlusTreeIndex *bindex = static_cast<BPlusTreeIndex *>((*it)->GetIndex());
        vector<RowId> results;
        bindex->ScanKey(temp_row, results, nullptr, "=");
        if (!((results.size() == 1 && results[0] == old_rowid) || results.size() == 0)) {
          cout << "Error: Primary key columns can't repeat" << endl;
          return false;
        }  // repeat self or not found: it's ok
        // LOG(INFO) << "Primary not repeat or null.";
      }
      // unique check
      /*auto*/ columns = tableInfo->GetSchema()->GetColumns();
      for (auto column : columns) {
        if (!column->IsUnique() || !column->IsNullable())  // if not nullable, that must be primary key, which is checked above (not null not supported)
          continue;
        else {
          vector<Field> fields;
          fields.push_back(*(new_row.GetField(column->GetTableInd())));
          Row temp_row(fields);
          auto it = indexes.begin();
          // for (; it != indexes.end() && (*it)->GetIndexName() != (string("UNIQUE_") + column->GetName()); it++);
          for (; it != indexes.end() && !(*it)->GetIndexKeySchema()->GetColumn(0)->IsUnique(); it++)
            ;
          BPlusTreeIndex *bindex = static_cast<BPlusTreeIndex *>((*it)->GetIndex());
          vector<RowId> results;
          bindex->ScanKey(temp_row, results, nullptr, "=");
          if (!((results.size() == 1 && results[0] == old_rowid) || results.size() == 0)) {
            cout << "Error: Unique key columns can't repeat" << endl;
            return false;
          }  // repeat self or not found: it's ok
          // LOG(INFO) << "Unique not repeat.";
        }
      }
    }
    if(!tableHeap->UpdateTuple(new_row, old_rowid, nullptr))
    {
      cout << "Delete failed." << endl;
      return false;
    }
    // remove old one and insert new one from indexes
    for(auto index:indexes)
    {
      int i = 0, index_column_num = index->GetIndexKeySchema()->GetColumnCount();
      uint32_t *column_indexes = new uint32_t[index_column_num];
      for(auto name:index->GetIndexKeySchema()->GetColumns())
      {
        tableInfo->GetSchema()->GetColumnIndex(name->GetName(), column_indexes[i++]);
      }
      vector<Field> fields_old, fields_new;
      for(auto i = 0; i < index_column_num; ++i)
      {
        fields_old.push_back(*(old_row.GetField(column_indexes[i])));
        fields_new.push_back(*(new_row.GetField(column_indexes[i])));
      }
      Row remove_key(fields_old), insert_key(fields_new);
      index->GetIndex()->RemoveEntry(remove_key, old_rowid, nullptr);
      //LOG(INFO) << "remove entry success";
      index->GetIndex()->InsertEntry(insert_key, old_rowid, nullptr);
      //LOG(INFO) << "insert entry success";
      delete[] column_indexes;
    }
    return true;
  }
  else
  {
    return false;
  }
}

Row UpdateExecutor::GenerateUpdatedTuple(const Row &src_row) {
//  auto updateAttr = plan_->GetUpdateAttr();
//  vector<Field> new_fields;
//  for(size_t i=0; i < src_row.GetFieldCount(); i++){
//    bool ifUpdate = false;
//    for(const auto& attr: updateAttr) {
//      if (i == attr.first){
//        auto &const_value = reinterpret_cast<ConstantValueExpression *>(attr.second.get())->val_;
//        new_fields.push_back(const_value);
//        ifUpdate = true;
//        break;
//      }
//    }
//    if(!ifUpdate){
//      new_fields.push_back(*src_row.GetField(i));
//    }
//  }
//  if(new_fields.size() != src_row.GetFieldCount()){
//    LOG(ERROR) << "Error generate tuple.";
//  }
//  Row new_row(new_fields);
//  return new_row;
  auto update_attr =  plan_->GetUpdateAttr();
  vector<Field> fields;
  for(auto column:plan_->OutputSchema()->GetColumns())
  {
    auto it = update_attr.begin();
    it= update_attr.find(column->GetTableInd());
    if(it != update_attr.end())
    {
      //LOG(INFO) << "find update column " << column->GetName();
      auto data = dynamic_pointer_cast<ConstantValueExpression>(((*it).second));
      fields.push_back(data->val_);
    }
    else
    {
      //LOG(INFO) << "not update column " << column->GetName();
      fields.push_back(*(src_row.GetField(column->GetTableInd())));
    }
  }
  return Row(fields);
}