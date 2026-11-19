//
// Created by njz on 2023/1/30.
//

#include "executor/executors/update_executor.h"
#include "planner/expressions/constant_value_expression.h"
UpdateExecutor::UpdateExecutor(ExecuteContext *exec_ctx, const UpdatePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void UpdateExecutor::Init() {
  auto *catalog = exec_ctx_->GetCatalog();
  if (catalog->GetTable(plan_->GetTableName(), table_) != DB_SUCCESS) {
    throw std::runtime_error("Update table not found");
  }
  index_info_.clear();
  catalog->GetTableIndexes(plan_->GetTableName(), index_info_);
  std::vector<bool> covered(table_->GetSchema()->GetColumnCount(), false);
  for (auto *index : index_info_) {
    const bool primary = index->GetIndexName() == plan_->GetTableName() + "_primary_index";
    const auto *schema = index->GetIndexKeySchema();
    if (primary || schema->GetColumnCount() == 1) {
      for (auto *column : schema->GetColumns()) {
        covered[column->GetTableInd()] = true;
      }
    }
  }
  for (auto *column : table_->GetSchema()->GetColumns()) {
    if (column->IsUnique() && !covered[column->GetTableInd()]) {
      throw std::runtime_error("Missing constraint index for column " + column->GetName());
    }
  }
  child_executor_->Init();
}

bool UpdateExecutor::Next(Row *, RowId *) {
  Row old_row;
  RowId old_rid;
  if (!child_executor_->Next(&old_row, &old_rid)) {
    return false;
  }

  Row new_row = GenerateUpdatedTuple(old_row);
  auto *txn = exec_ctx_->GetTransaction();
  // MiniSQL's B+ tree stores one row ID per key. Check each tree with its own key layout.
  for (auto *index : index_info_) {
    Row key;
    new_row.GetKeyFromRow(table_->GetSchema(), index->GetIndexKeySchema(), key);
    if (index->GetIndexName() == plan_->GetTableName() + "_primary_index") {
      for (auto *field : key.GetFields()) {
        if (field->IsNull()) {
          throw std::runtime_error("Primary key columns cannot be NULL");
        }
      }
    }
    std::vector<RowId> matches;
    auto status = index->GetIndex()->ScanKey(key, matches, txn);
    if (status != DB_SUCCESS && status != DB_KEY_NOT_FOUND) {
      throw std::runtime_error("Failed to check index " + index->GetIndexName());
    }
    for (const auto &match : matches) {
      if (!(match == old_rid)) {
        throw std::runtime_error("Duplicate entry for key " + index->GetIndexName());
      }
    }
  }

  if (!table_->GetTableHeap()->UpdateTuple(new_row, old_rid, txn)) {
    throw std::runtime_error("Failed to update tuple");
  }
  for (auto *index : index_info_) {
    Row old_key, new_key;
    old_row.GetKeyFromRow(table_->GetSchema(), index->GetIndexKeySchema(), old_key);
    new_row.GetKeyFromRow(table_->GetSchema(), index->GetIndexKeySchema(), new_key);
    index->GetIndex()->RemoveEntry(old_key, old_rid, txn);
    index->GetIndex()->InsertEntry(new_key, new_row.GetRowId(), txn);
  }
  return true;
}

Row UpdateExecutor::GenerateUpdatedTuple(const Row &src_row) {
  const auto &updates = plan_->GetUpdateAttr();
  std::vector<Field> fields;
  for (uint32_t i = 0; i < src_row.GetFieldCount(); ++i) {
    auto update = updates.find(i);
    if (update == updates.end()) {
      fields.push_back(*src_row.GetField(i));
    } else {
      fields.emplace_back(update->second->Evaluate(&src_row));
    }
  }
  return Row(fields);
}
