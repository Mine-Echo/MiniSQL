#include "executor/execute_engine.h"

#include <dirent.h>
#include <sys/stat.h>
#include <sys/types.h>

#include <chrono>

#include "common/result_writer.h"
#include "executor/executors/delete_executor.h"
#include "executor/executors/index_scan_executor.h"
#include "executor/executors/insert_executor.h"
#include "executor/executors/seq_scan_executor.h"
#include "executor/executors/update_executor.h"
#include "executor/executors/values_executor.h"
#include "glog/logging.h"
#include "planner/planner.h"
#include "utils/utils.h"

extern "C" {
extern int yyparse(void);
#include "parser/minisql_lex.h"
#include "parser/parser.h"
}

ExecuteEngine::ExecuteEngine() {
  char path[] = "./databases";
  DIR *dir;
  if((dir = opendir(path)) == nullptr) {
    mkdir("./databases", 0777);
    dir = opendir(path);
  }
  /** When you have completed all the code for
     *  the test, run it using main.cpp and uncomment
     *  this part of the code.
  struct dirent *stdir;
  while((stdir = readdir(dir)) != nullptr) {
    if( strcmp( stdir->d_name , "." ) == 0 ||
        strcmp( stdir->d_name , "..") == 0 ||
        stdir->d_name[0] == '.')
      continue;
    dbs_[stdir->d_name] = new DBStorageEngine(stdir->d_name, false);
  }
   **/
  closedir(dir);
}

std::unique_ptr<AbstractExecutor> ExecuteEngine::CreateExecutor(ExecuteContext *exec_ctx,
                                                                const AbstractPlanNodeRef &plan) {
  switch (plan->GetType()) {
    // Create a new sequential scan executor
    case PlanType::SeqScan: {
      return std::make_unique<SeqScanExecutor>(exec_ctx, dynamic_cast<const SeqScanPlanNode *>(plan.get()));
    }
    // Create a new index scan executor
    case PlanType::IndexScan: {
      return std::make_unique<IndexScanExecutor>(exec_ctx, dynamic_cast<const IndexScanPlanNode *>(plan.get()));
    }
    // Create a new update executor
    case PlanType::Update: {
      auto update_plan = dynamic_cast<const UpdatePlanNode *>(plan.get());
      auto child_executor = CreateExecutor(exec_ctx, update_plan->GetChildPlan());
      return std::make_unique<UpdateExecutor>(exec_ctx, update_plan, std::move(child_executor));
    }
      // Create a new delete executor
    case PlanType::Delete: {
      auto delete_plan = dynamic_cast<const DeletePlanNode *>(plan.get());
      auto child_executor = CreateExecutor(exec_ctx, delete_plan->GetChildPlan());
      return std::make_unique<DeleteExecutor>(exec_ctx, delete_plan, std::move(child_executor));
    }
    case PlanType::Insert: {
      auto insert_plan = dynamic_cast<const InsertPlanNode *>(plan.get());
      auto child_executor = CreateExecutor(exec_ctx, insert_plan->GetChildPlan());
      return std::make_unique<InsertExecutor>(exec_ctx, insert_plan, std::move(child_executor));
    }
    case PlanType::Values: {
      return std::make_unique<ValuesExecutor>(exec_ctx, dynamic_cast<const ValuesPlanNode *>(plan.get()));
    }
    default:
      throw std::logic_error("Unsupported plan type.");
  }
}

dberr_t ExecuteEngine::ExecutePlan(const AbstractPlanNodeRef &plan, std::vector<Row> *result_set, Transaction *txn,
                                   ExecuteContext *exec_ctx) {
  // Construct the executor for the abstract plan node
  auto executor = CreateExecutor(exec_ctx, plan);

  try {
    executor->Init();
    RowId rid{};
    Row row{};
    while (executor->Next(&row, &rid)) {
      if (result_set != nullptr) {
        result_set->push_back(row);
      }
    }
  } catch (const exception &ex) {
    std::cout << "Error Encountered in Executor Execution: " << ex.what() << std::endl;
    if (result_set != nullptr) {
      result_set->clear();
    }
    return DB_FAILED;
  }
  return DB_SUCCESS;
}

dberr_t ExecuteEngine::Execute(pSyntaxNode ast) {
  if (ast == nullptr) {
    return DB_FAILED;
  }
  auto start_time = std::chrono::system_clock::now();
  unique_ptr<ExecuteContext> context(nullptr);
  if(!current_db_.empty())
    context = dbs_[current_db_]->MakeExecuteContext(nullptr);
  switch (ast->type_) {
    case kNodeCreateDB:
      return ExecuteCreateDatabase(ast, context.get());
    case kNodeDropDB:
      return ExecuteDropDatabase(ast, context.get());
    case kNodeShowDB:
      return ExecuteShowDatabases(ast, context.get());
    case kNodeUseDB:
      return ExecuteUseDatabase(ast, context.get());
    case kNodeShowTables:
      return ExecuteShowTables(ast, context.get());
    case kNodeCreateTable:
      return ExecuteCreateTable(ast, context.get());
    case kNodeDropTable:
      return ExecuteDropTable(ast, context.get());
    case kNodeShowIndexes:
      return ExecuteShowIndexes(ast, context.get());
    case kNodeCreateIndex:
      return ExecuteCreateIndex(ast, context.get());
    case kNodeDropIndex:
      return ExecuteDropIndex(ast, context.get());
    case kNodeTrxBegin:
      return ExecuteTrxBegin(ast, context.get());
    case kNodeTrxCommit:
      return ExecuteTrxCommit(ast, context.get());
    case kNodeTrxRollback:
      return ExecuteTrxRollback(ast, context.get());
    case kNodeExecFile:
      return ExecuteExecfile(ast, context.get());
    case kNodeQuit:
      return ExecuteQuit(ast, context.get());
    default:
      break;
  }
  // Plan the query.
  Planner planner(context.get());
  std::vector<Row> result_set{};
  try {
    planner.PlanQuery(ast);
    // Execute the query.
    ExecutePlan(planner.plan_, &result_set, nullptr, context.get());
  } catch (const exception &ex) {
    std::cout << "Error Encountered in Planner: " << ex.what() << std::endl;
    return DB_FAILED;
  }
  auto stop_time = std::chrono::system_clock::now();
  double duration_time =
      double((std::chrono::duration_cast<std::chrono::milliseconds>(stop_time - start_time)).count());
  // Return the result set as string.
  std::stringstream ss;
  ResultWriter writer(ss);

  if (planner.plan_->GetType() == PlanType::SeqScan || planner.plan_->GetType() == PlanType::IndexScan) {
    auto schema = planner.plan_->OutputSchema();
    auto num_of_columns = schema->GetColumnCount();
    if (!result_set.empty()) {
      // find the max width for each column
      vector<int> data_width(num_of_columns, 0);
      for (const auto &row : result_set) {
        for (uint32_t i = 0; i < num_of_columns; i++) {
          data_width[i] = max(data_width[i], int(row.GetField(i)->toString().size()));
        }
      }
      int k = 0;
      for (const auto &column : schema->GetColumns()) {
        data_width[k] = max(data_width[k], int(column->GetName().length()));
        k++;
      }
      // Generate header for the result set.
      writer.Divider(data_width);
      k = 0;
      writer.BeginRow();
      for (const auto &column : schema->GetColumns()) {
        writer.WriteHeaderCell(column->GetName(), data_width[k++]);
      }
      writer.EndRow();
      writer.Divider(data_width);

      // Transforming result set into strings.
      for (const auto &row : result_set) {
        writer.BeginRow();
        for (uint32_t i = 0; i < schema->GetColumnCount(); i++) {
          writer.WriteCell(row.GetField(i)->toString(), data_width[i]);
        }
        writer.EndRow();
      }
      writer.Divider(data_width);
    }
    writer.EndInformation(result_set.size(), duration_time, true);
  } else {
    writer.EndInformation(result_set.size(), duration_time, false);
  }
  std::cout << writer.stream_.rdbuf();
  return DB_SUCCESS;
}

void ExecuteEngine::ExecuteInformation(dberr_t result) {
  switch (result) {
    case DB_ALREADY_EXIST:
      cout << "Database already exists." << endl;
      break;
    case DB_NOT_EXIST:
      cout << "Database not exists." << endl;
      break;
    case DB_TABLE_ALREADY_EXIST:
      cout << "Table already exists." << endl;
      break;
    case DB_TABLE_NOT_EXIST:
      cout << "Table not exists." << endl;
      break;
    case DB_INDEX_ALREADY_EXIST:
      cout << "Index already exists." << endl;
      break;
    case DB_INDEX_NOT_FOUND:
      cout << "Index not exists." << endl;
      break;
    case DB_COLUMN_NAME_NOT_EXIST:
      cout << "Column not exists." << endl;
      break;
    case DB_KEY_NOT_FOUND:
      cout << "Key not exists." << endl;
      break;
    case DB_QUIT:
      cout << "Bye." << endl;
      break;
    default:
      break;
  }
}
/**
 * TODO: Student Implement
 */
dberr_t ExecuteEngine::ExecuteCreateDatabase(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteCreateDatabase" << std::endl;
#endif
  if(ast->child_== nullptr) return DB_FAILED;
  std::string db_name(ast->child_->val_);
  if(this->dbs_.find(db_name)!=dbs_.end())
    return DB_ALREADY_EXIST;
  dbs_.insert(std::pair<string,DBStorageEngine*>(db_name,new DBStorageEngine(db_name)));
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t ExecuteEngine::ExecuteDropDatabase(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteDropDatabase" << std::endl;
#endif
 if(ast->child_== nullptr) return DB_FAILED;
 std::string db_name(ast->child_->val_);
 if(dbs_.find(db_name)==dbs_.end())
   return DB_NOT_EXIST;
 delete dbs_[db_name];
 dbs_.erase(db_name);
 if(current_db_==db_name)
   current_db_.clear();
 std::string db_file_name="./database/"+db_name;
 remove(db_file_name.c_str());
 std::cout<<"Database '"<<db_name<<"' dropped"<<std::endl;
 return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t ExecuteEngine::ExecuteShowDatabases(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteShowDatabases" << std::endl;
#endif
  for(auto db:dbs_)
    std::cout<<db.first<<std::endl;
  //std::cout<<"Database number: "<<dbs_.size()<<std::endl;
  std::cout<<dbs_.size()<<" rows in set"<<std::endl;
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t ExecuteEngine::ExecuteUseDatabase(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteUseDatabase" << std::endl;
#endif
  if(ast->child_== nullptr) return DB_FAILED;
  std:string db_name(ast->child_->val_);
  if(dbs_.find(db_name)==dbs_.end())
   return DB_NOT_EXIST;
  current_db_=db_name;
  std::cout<<"Database changed"<<std::endl;
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t ExecuteEngine::ExecuteShowTables(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteShowTables" << std::endl;
#endif
  if(dbs_.find(current_db_)==dbs_.end()){
   std::cout<<"No database selected"<<std::endl;
   return DB_FAILED;
  }
  DBStorageEngine* dbse=dbs_[current_db_];
  vector<TableInfo*> tables;
  dbse->catalog_mgr_->GetTables(tables);
  for(auto table:tables)
    std::cout<<table->GetTableName()<<std::endl;
  std::cout<<tables.size()<<" rows in set"<<std::endl;
  return DB_FAILED;
}

/**
 * TODO: Student Implement
 */
dberr_t ExecuteEngine::ExecuteCreateTable(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteCreateTable" << std::endl;
#endif
  if(ast->child_== nullptr) return DB_FAILED;
  if(dbs_.find(current_db_)==dbs_.end()){
    std::cout<<"No database selected"<<std::endl;
    return DB_FAILED;
  }
  DBStorageEngine *dbse=dbs_[current_db_];
  ASSERT(dbse!= nullptr,"wrong");
  string table_name=ast->child_->val_;
  TableInfo* table_info= nullptr;
  if(dbse->catalog_mgr_->GetTable(table_name,table_info)==DB_SUCCESS){
    cout<<"Table '"<<table_name<<"' already exists"<<endl;
    return DB_TABLE_ALREADY_EXIST;
  };
  pSyntaxNode column=ast->child_->next_->child_;
  vector<Column *> columns;
  vector<string >column_names;
  vector<string>unique_keys;
  vector<string>primary_keys;
  unordered_map<string,bool>is_unique;
  unordered_map<string,bool>is_primary;
  unordered_map<string,string>type;
  unordered_map<string,int>string_size;
  while(column!= nullptr&&column->type_==kNodeColumnDefinition){
//    bool is_unique_flag=false;
//    if(column->val_!= nullptr) is_unique_flag=true;
    string column_name=column->child_->val_;
    string column_type=column->child_->next_->val_;
    column_names.push_back(column_name);
    is_unique[column_name]= (column->val_ != nullptr);
    is_primary[column_name]=false;
    type[column_name]=column_type;
    if(is_unique[column_name])
      unique_keys.push_back(column_name);
    if(column_type=="char"){
      string_size[column_name]=atoi(column->child_->next_->child_->val_);
      if(string_size[column_name]<0){
        cout<<"char size <= 0"<<endl;
        return DB_FAILED;
      }
    }
    column=column->next_;
  }
  if(column!= nullptr){
    auto key_node=column->child_;
    while(key_node){
      string primary_key_name=key_node->val_;
      is_primary[primary_key_name]=true;
      //is_unique[primary_key_name]=true;
      primary_keys.push_back(primary_key_name);
      //unique_keys.push_back(primary_key_name);
      key_node=key_node->next_;
    }
  }
  //create column
  for(int i=0;i<column_names.size();i++){
    Column* column;
    string column_name=column_names[i];
    if(type[column_name]=="int")
      column=new Column(column_name,TypeId::kTypeInt,i,
                          true,is_unique[column_name]||is_primary[column_name]);
    else if(type[column_name]=="float")
      column=new Column(column_name,TypeId::kTypeFloat,i,
                          true,is_unique[column_name]||is_primary[column_name]);
    else if(type[column_name]=="char")
      column=new Column(column_name,TypeId::kTypeChar,string_size[column_name],i,
                          true,is_unique[column_name]||is_primary[column_name]);
    else{
      cout<<"unknown type"<<endl;
      return DB_FAILED;
    }
    columns.push_back(column);
  }
  Schema* schema=new Schema(columns);
  dberr_t is_success=dbse->catalog_mgr_->CreateTable(table_name,schema,nullptr,table_info);
  if(is_success!=DB_SUCCESS){
    cout<<"Create error"<<endl;
    return is_success;
  }
  //create index
  IndexInfo* index_info;
  //primary index
  is_success=dbse->catalog_mgr_->CreateIndex(table_name,table_name+"_primary_index",primary_keys, nullptr,index_info,"bptree");
  if(is_success!=DB_SUCCESS){
    cout<<"Create index failed"<<endl;
    return is_success;
  }
//  unique index
  for(auto key:unique_keys){
    vector<string> index_key;
    index_key.push_back(key);
    is_success=dbse->catalog_mgr_->CreateIndex(table_name,table_name+"_unique_index_"+key,index_key, nullptr,index_info,"bptree");
    if(is_success!=DB_SUCCESS){
      cout<<"Create index failed"<<endl;
      return is_success;
    }
  }
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t ExecuteEngine::ExecuteDropTable(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteDropTable" << std::endl;
#endif
  if(ast->child_== nullptr) return DB_FAILED;
  if(current_db_.empty()){
    cout<<"No database selected"<<endl;
    return DB_FAILED;
  }
  string table_name(ast->child_->val_);
  dberr_t is_success=dbs_[current_db_]->catalog_mgr_->DropTable(table_name);
  if(is_success==DB_SUCCESS)
    cout<<"Table '"<<table_name<<"' dropped."<<endl;
  return is_success;
}

/**
 * TODO: Student Implement
 */
dberr_t ExecuteEngine::ExecuteShowIndexes(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteShowIndexes" << std::endl;
#endif
  if(current_db_.empty()){
    cout<<"No database selected."<<endl;
    return DB_FAILED;
  }
  //get indexes
  vector<TableInfo*> table_infos;
  dbs_[current_db_]->catalog_mgr_->GetTables(table_infos);
  for(TableInfo* table_info:table_infos){
    vector<IndexInfo *> index_infos;
    dbs_[current_db_]->catalog_mgr_->GetTableIndexes(table_info->GetTableName(),index_infos);
    for(auto index_info:index_infos)
      cout<<index_info->GetIndexName()<<endl;
  }
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t ExecuteEngine::ExecuteCreateIndex(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteCreateIndex" << std::endl;
#endif
  if(current_db_.empty()){
    cout<<"No database selected"<<endl;
    return DB_FAILED;
  }
  //get index_name
  auto index_name_node=ast->child_;
  string index_name(index_name_node->val_);
  //get table name
  auto table_name_node=index_name_node->next_;
  string table_name(table_name_node->val_);
  //get index_keys
  vector<string> index_keys;
  auto index_keys_node=table_name_node->next_->child_;
  while(index_keys_node!= nullptr){
    index_keys.emplace_back(string(index_keys_node->val_));
    index_keys_node=index_keys_node->next_;
  }
  //index_info
  IndexInfo* index_info= nullptr;
  dberr_t is_success=context->GetCatalog()->CreateIndex(table_name,index_name,index_keys, context->GetTransaction(),index_info,"bptree");
  if(is_success==DB_SUCCESS){
    cout<<"Index '"<<index_name<<"' created"<<endl;
  }
  return is_success;
//  if(current_db_.empty()){
//    return DB_FAILED;
//  }
//
//  //get the index name
//  pSyntaxNode index_name_ptr = ast->child_;
//  string index_name = index_name_ptr->val_;
//
//  //get the index table
//  pSyntaxNode index_table_ptr = index_name_ptr->next_;
//  string table_name = index_table_ptr->val_;
//
//  //get the index key
//  vector<string> index_keys;
//  pSyntaxNode index_key_ptr = index_table_ptr->next_;
//  pSyntaxNode ptr = index_key_ptr->child_;
//  while(ptr != nullptr){
//    index_keys.emplace_back(ptr->val_);
//    ptr = ptr->next_;
//  }
//
//  TableInfo *table_info = nullptr;
//  context->GetCatalog()->GetTable(table_name, table_info);
//  if(index_keys.size() == 1){
//    uint32_t col_index;
//    if(table_info->GetSchema()->GetColumnIndex(index_keys[0], col_index) == DB_SUCCESS){
//      const Column *column = table_info->GetSchema()->GetColumn(col_index);
//      if(!column->IsUnique()){
//        cout << "The index is not unique." << endl;
//        return DB_FAILED;
//      }
//    }
//  }
//  else{
//    for(const auto& index : index_keys){
//      uint32_t col_index;
//      if(table_info->GetSchema()->GetColumnIndex(index, col_index) == DB_SUCCESS){
//        const Column *column = table_info->GetSchema()->GetColumn(col_index);
//        if(column->IsUnique()){
//          break;
//        }
//        if(index == index_keys[index_keys.size()-1]){
//          cout << "The indexes don't have unique index." << endl;
//          return DB_FAILED;
//        }
//      }
//    }
//  }
//
//  //get the index type
//  pSyntaxNode index_type_ptr = index_key_ptr->next_;
//  string index_type = "bptree";
//  if(index_type_ptr != nullptr){
//    index_type = index_type_ptr->child_->val_;
//  }
//
//  //create index
//  IndexInfo *index_info = nullptr;
//  dberr_t result = context->GetCatalog()->CreateIndex(table_name, index_name, index_keys, context->GetTransaction(), index_info, index_type);
//  if(result == DB_SUCCESS){
//    cout << "Index '" + index_name + "' created." << endl;
//  }
//  return result;
}

/**
 * TODO: Student Implement
 */
dberr_t ExecuteEngine::ExecuteDropIndex(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteDropIndex" << std::endl;
#endif
  //index name
  auto node=ast->child_;
  if(current_db_.empty()){
    cout<<"No database selected."<<endl;
    return DB_FAILED;
  }
  string index_name(ast->child_->val_);
  vector<TableInfo*> table_infos;
  dbs_[current_db_]->catalog_mgr_->GetTables(table_infos);
  for(auto table_info:table_infos){
    if(dbs_[current_db_]->catalog_mgr_->DropIndex(table_info->GetTableName(),index_name)==DB_SUCCESS)
      return DB_SUCCESS;
  }
  cout<<"Drop error"<<endl;
  return DB_FAILED;
}


dberr_t ExecuteEngine::ExecuteTrxBegin(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteTrxBegin" << std::endl;
#endif
  return DB_FAILED;
}

dberr_t ExecuteEngine::ExecuteTrxCommit(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteTrxCommit" << std::endl;
#endif
  return DB_FAILED;
}

dberr_t ExecuteEngine::ExecuteTrxRollback(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteTrxRollback" << std::endl;
#endif
  return DB_FAILED;
}

/**
 * TODO: Student Implement
 */
dberr_t ExecuteEngine::ExecuteExecfile(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteExecfile" << std::endl;
#endif
  string filename=ast->child_->val_;
  ifstream exeFile(filename, ios::binary);

  if(!exeFile.is_open()){
    cout<<"file can't be opened."<<endl;
    return DB_FAILED;
  }
  clock_t start_time=clock();
  while(1){
    string line;
    string new_line;
    char command[1024];
    if(exeFile.eof()) {
      clock_t end_time=clock();
      cout << "The run time is: " <<(double)(end_time - start_time) / CLOCKS_PER_SEC << "s" << endl;
      return DB_SUCCESS;
    }
    exeFile.getline(command,1024);

    line = command;
    //delete the '\r'
    if(find(line.begin(),line.end(), ';') == line.end()){//no ;
      auto iter = find(line.begin(), line.end(), '\r');
      line.assign(line.begin(), iter);
      new_line += line;
      continue;
    }
    else{
      auto iter = find(line.begin(), line.end(), '\r');
      line.assign(line.begin(), iter);
      new_line += line;
    }

    YY_BUFFER_STATE bp = yy_scan_string(new_line.c_str());

    if (bp == nullptr) {
      LOG(ERROR) << "Failed to create yy buffer state." << std::endl;
      exit(1);
    }
    yy_switch_to_buffer(bp);
    MinisqlParserInit();
    yyparse();
    if (MinisqlParserGetError()) {
      // error
      printf("%s\n", MinisqlParserGetErrorMessage());
    }else {
      // Comment them out if you don't need to debug the syntax tree
      //printf("[INFO] Sql syntax parse ok!\n");
    }

    ExecuteEngine engine;
    auto result = Execute(MinisqlGetParserRootNode());
    // clean memory after parse
    MinisqlParserFinish();
    yy_delete_buffer(bp);
    yylex_destroy();
    // quit condition
    ExecuteInformation(result);
    if (result == DB_QUIT) {
      return DB_QUIT;
    }
  }
//  string filename=ast->child_->val_;
//  fstream exeFile(filename);
//
//  if(!exeFile.is_open()){
//    std::cout<<"file can't be opened"<<endl;
//    return DB_FAILED;
//  }
//  char* command= new char[1024];
//  while(true){
//    if(exeFile.eof()) return DB_SUCCESS;
//    exeFile.getline(command,1024);
//    YY_BUFFER_STATE bp = yy_scan_string(command);
//    if (bp == nullptr) {
//      LOG(ERROR) << "Failed to create yy buffer state." << std::endl;
//      exit(1);
//    }
//    yy_switch_to_buffer(bp);
//    MinisqlParserInit();
//    yyparse();
//    if (MinisqlParserGetError()) {
//      // error
//      printf("%s\n", MinisqlParserGetErrorMessage());
//    }
//
//    ExecuteEngine engine;
//    auto result = engine.Execute(MinisqlGetParserRootNode());
//    // clean memory after parse
//    MinisqlParserFinish();
//    yy_delete_buffer(bp);
//    yylex_destroy();
//    // quit condition
//    engine.ExecuteInformation(result);
//    if (result == DB_QUIT) {
//      return DB_QUIT;
//    }
//  }
}

/**
 * TODO: Student Implement
 */
dberr_t ExecuteEngine::ExecuteQuit(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteQuit" << std::endl;
#endif
  if(ast->type_==kNodeQuit)
    return DB_QUIT;
  return DB_FAILED;
}
