#include "storage/table_heap.h"

#include <unordered_map>
#include <vector>

#include "common/instance.h"
#include "gtest/gtest.h"
#include "record/field.h"
#include "record/schema.h"
#include "utils/utils.h"

static string db_file_name = "table_heap_test.db";
using Fields = std::vector<Field>;

TEST(TableHeapTest, TableHeapSampleTest) {
  // init testing instance
  auto disk_mgr_ = new DiskManager(db_file_name);
  auto bpm_ = new BufferPoolManager(DEFAULT_BUFFER_POOL_SIZE, disk_mgr_);
  const int row_nums = 10000;
  // create schema
  std::vector<Column *> columns = {new Column("id", TypeId::kTypeInt, 0, false, false),
                                   new Column("name", TypeId::kTypeChar, 64, 1, true, false),
                                   new Column("account", TypeId::kTypeFloat, 2, true, false)};
  auto schema = std::make_shared<Schema>(columns);
  // create rows
  std::unordered_map<int64_t, Fields *> row_values;
  uint32_t size = 0;
  TableHeap *table_heap = TableHeap::Create(bpm_, schema.get(), nullptr, nullptr, nullptr);
  for (int i = 0; i < row_nums; i++) {
    int32_t len = RandomUtils::RandomInt(0, 64);
    char *characters = new char[len];
    RandomUtils::RandomString(characters, len);
    Fields *fields =
        new Fields{Field(TypeId::kTypeInt, i), Field(TypeId::kTypeChar, const_cast<char *>(characters), len, true),
                   Field(TypeId::kTypeFloat, RandomUtils::RandomFloat(-999.f, 999.f))};
    Row row(*fields);
    ASSERT_TRUE(table_heap->InsertTuple(row, nullptr));
    if (row_values.find(row.GetRowId().Get()) != row_values.end()) {
      std::cout << row.GetRowId().Get() << std::endl;
      ASSERT_TRUE(false);
    } else {
      row_values.emplace(row.GetRowId().Get(), fields);
      size++;
    }
    delete[] characters;
  }

  ASSERT_EQ(row_nums, row_values.size());
  ASSERT_EQ(row_nums, size);
  for (auto row_kv : row_values) {
    size--;
    Row row(RowId(row_kv.first));
    table_heap->GetTuple(&row, nullptr);
    ASSERT_EQ(schema.get()->GetColumnCount(), row.GetFields().size());
    for (size_t j = 0; j < schema.get()->GetColumnCount(); j++) {
      ASSERT_EQ(CmpBool::kTrue, row.GetField(j)->CompareEquals(row_kv.second->at(j)));
    }
    // free spaces
    delete row_kv.second;
  }
  ASSERT_EQ(size, 0);
}

TEST(TableHeapTest, IteratorCopyAssignment) {
  DBStorageEngine engine("iterator_copy_test.db", true, 8);
  Schema schema({new Column("id", kTypeInt, 0, false, false)});
  std::unique_ptr<TableHeap> heap(TableHeap::Create(engine.bpm_, &schema, nullptr, nullptr, nullptr));
  for (int id : {1, 2}) {
    Fields fields{Field(kTypeInt, id)};
    Row row(fields);
    ASSERT_TRUE(heap->InsertTuple(row, nullptr));
  }

  TableIterator assigned;
  EXPECT_EQ(INVALID_ROWID, assigned->GetRowId());
  auto first = heap->Begin(nullptr);
  EXPECT_EQ(&assigned, &(assigned = first));
  const TableIterator &alias = assigned;
  EXPECT_EQ(&assigned, &(assigned = alias));
  ASSERT_EQ(1U, assigned->GetFieldCount());
  EXPECT_EQ(kTrue, assigned->GetField(0)->CompareEquals(Field(kTypeInt, 1)));

  auto previous = assigned++;
  EXPECT_EQ(first, previous);
  EXPECT_EQ(kTrue, first->GetField(0)->CompareEquals(Field(kTypeInt, 1)));
  EXPECT_EQ(kTrue, assigned->GetField(0)->CompareEquals(Field(kTypeInt, 2)));
  ++previous;
  EXPECT_EQ(assigned, previous);
  EXPECT_NE(first, previous);
  assigned = first;
  EXPECT_EQ(first, assigned);
}
