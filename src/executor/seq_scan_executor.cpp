//
// Created by njz on 2023/1/17.
//
#include "executor/executors/seq_scan_executor.h"

/**
* TODO: Student Implement
*/
SeqScanExecutor::SeqScanExecutor(ExecuteContext *exec_ctx, const SeqScanPlanNode *plan)
    : AbstractExecutor(exec_ctx),
      plan_(plan){
  //RowId rid(INVALID_PAGE_ID,0);
  //this->itr.TableIterator::TableIterator(nullptr,rid);//itr没有默认构造，必须初始化一下
}

void SeqScanExecutor::Init() {
  if (exec_ctx_->GetCatalog()->GetTable(plan_->GetTableName(), table_info_) != DB_SUCCESS){
    LOG(WARNING) << "Get table name fail." << endl;
    return;
  }
  this->itr=table_info_->GetTableHeap()->Begin(exec_ctx_->GetTransaction());
//  if(exec_ctx_->GetCatalog()->GetTable(plan_->GetTableName(), table_info) != DB_SUCCESS){
//    LOG(WARNING) << "Get table name fail.";
//    exit(1);
//  }
//  iter = table_info->GetTableHeap()->Begin(exec_ctx_->GetTransaction());
}

bool SeqScanExecutor::Next(Row *row, RowId *rid) {
  TableHeap* table_heap=table_info_->GetTableHeap();
  if(itr==table_heap->End())
    return false;
  Row tmp=*itr;
  if(plan_->GetPredicate()!=nullptr){
    for(;itr!=table_heap->End();itr++){
      tmp=*itr;
      Field field=plan_->GetPredicate()->Evaluate(&tmp);
      if(field.CompareEquals(Field(kTypeInt,1)))//equal
        break;
    }
  }
  if(itr==table_heap->End())return false;//没有了
  vector<Field> fields;
  const Schema* schema=plan_->OutputSchema();
  for(auto column:schema->GetColumns())
//    for(auto original_column:table_info_->GetSchema()->GetColumns())
//      if(column->GetName()==original_column->GetName())
        fields.push_back(*tmp.GetField(column->GetTableInd()));
  *row=Row(fields);
  *rid=RowId(tmp.GetRowId());
  itr++;
  return true;
//  Row temp_row;
//  TableHeap *table_heap = table_info->GetTableHeap();
//  if(plan_->GetPredicate() != nullptr){
//    for(; iter != table_heap->End(); iter++) {
//      temp_row = *iter
//      Field field = plan_->GetPredicate()->Evaluate(&temp_row);
//      if (field.CompareEquals(Field(kTypeInt, 1))) {
//        break;
//      }
//    }
//  }
//
//  if(iter == table_heap->End()){
//    //    iter = table_heap->Begin(exec_ctx_->GetTransaction());
//    return false;
//  }
//  vector<Field> fields;
//  const Schema *schema = plan_->OutputSchema();
//  for(auto column: schema->GetColumns())
//    for(auto old_column: table_info->GetSchema()->GetColumns()){
//      if(column->GetName() == old_column->GetName())
//        fields.push_back(*temp_row.GetField(old_column->GetTableInd()));
//    }
//
//  *row = Row(fields);
//  *rid = RowId(temp_row.GetRowId());
//  iter++;
//  return true;
}
