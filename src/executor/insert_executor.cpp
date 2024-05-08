//
// Created by njz on 2023/1/27.
//

#include "executor/executors/insert_executor.h"

InsertExecutor::InsertExecutor(ExecuteContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void InsertExecutor::Init() {
  child_executor_->Init();
}

bool InsertExecutor::Next([[maybe_unused]] Row *row, RowId *rid) {
  Row insert_row;
  RowId insert_rid;
  if(child_executor_->Next(&insert_row,&insert_rid)) {
    TableInfo *table_info = nullptr;
    if (this->exec_ctx_->GetCatalog()->GetTable(plan_->GetTableName(), table_info) != DB_SUCCESS) return false;
    // find duplicate key
    vector<IndexInfo *> indexes;
    if (exec_ctx_->GetCatalog()->GetTableIndexes(plan_->GetTableName(), indexes) == DB_SUCCESS) {
      // traverse all indexes
      for (auto index : indexes) {
        vector<uint32_t> column_ids;
        vector<Column *> index_columns = index->GetIndexKeySchema()->GetColumns();
        // get column id
        for (auto index_column : index_columns) {
          uint32_t column_id;
          if (table_info->GetSchema()->GetColumnIndex(index_column->GetName(), column_id) == DB_SUCCESS)
            column_ids.push_back(column_id);
        }
        // get fields
        vector<Field> fields;
        for (auto column_id : column_ids) fields.push_back(*insert_row.GetField(column_id));
        Row index_row(fields);
        vector<RowId> rids;
        // find duplicate key

        if (index->GetIndex()->ScanKey(index_row, rids, exec_ctx_->GetTransaction(), "=") == DB_SUCCESS) {
          if (rids.empty()) break;
          cout << "Duplicated entry for key" << index->GetIndexName() << endl;
          return false;
        }
      }
    }
    // no duplicate key
    // insert the row
    table_info->GetTableHeap()->InsertTuple(insert_row, exec_ctx_->GetTransaction());
    insert_rid = insert_row.GetRowId();
    // update indexes
    if (!indexes.empty()) {
      for (auto index : indexes) {
        vector<uint32_t> column_ids;
        vector<Column *> columns = index->GetIndexKeySchema()->GetColumns();
        for (auto column : columns) {
          uint32_t column_id;
          if (table_info->GetSchema()->GetColumnIndex(column->GetName(), column_id) == DB_SUCCESS)
            column_ids.push_back(column_id);
        }
        vector<Field> fields;
        for (auto column_id : column_ids) fields.push_back(*insert_row.GetField(column_id));
        Row index_row(fields);
        index->GetIndex()->InsertEntry(index_row, insert_rid, exec_ctx_->GetTransaction());
      }
    }
    return true;
  }
  return false;
}