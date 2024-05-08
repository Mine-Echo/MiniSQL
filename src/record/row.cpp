#include "record/row.h"

/**
 * TODO: Student Implement
 */
uint32_t Row::SerializeTo(char *buf, Schema *schema) const {
  ASSERT(schema != nullptr, "Invalid schema before serialize.");
  ASSERT(schema->GetColumnCount() == fields_.size(), "Fields size do not match schema's column size.");
  // replace with your code here
  uint32_t offset=0;
  uint32_t num=this->fields_.size();
  if(num==0) return offset;//如果num为0，直接返回，不要存
//  //写rowId
//  MACH_WRITE_INT32(buf+offset,rid_.GetPageId());
//  offset+=sizeof(int32_t);
//  MACH_WRITE_UINT32(buf+offset,rid_.GetSlotNum());
//  offset==sizeof(uint32_t);
  //1.存num
  MACH_WRITE_UINT32(buf+offset,num);
  offset+=sizeof(uint32_t);
  //算出需要多少bytes的bitmap
  uint32_t n=(num-1)/8*8+1;
  char* bitmap=new char[n];
  for(uint32_t i=0;i<num;i++){
    if(this->fields_[i]->IsNull()) bitmap[i/8]&=~(0x01<<(i%8));//空的话设置位0
    else bitmap[i/8]|=(0x01<<(i%8));
  }
  //2.存bitmap，长度可以不存，根据num推算出来
  memcpy(buf+offset,bitmap,n);
  offset+=n;
  delete[] bitmap;
  //3.存field
  for(uint32_t i=0;i<num;i++){
    offset+=this->fields_[i]->SerializeTo(buf+offset);
  }
  return offset;
}

uint32_t Row::DeserializeFrom(char *buf, Schema *schema) {
  ASSERT(schema != nullptr, "Invalid schema before serialize.");
  //ASSERT(fields_.empty(), "Non empty field in row.");
  // replace with your code here
  //1.读取num,不用考虑num=0的情况，因为table_page在插入row时做了限制，num=0时不会插进去
  uint32_t offset=0;
  uint32_t num=MACH_READ_UINT32(buf+offset);
  offset+=sizeof(uint32_t);
  if(num==0) return sizeof(uint32_t);
  //2.读取bitmap
  uint32_t n=(num-1)/8*8+1;
  char* bitmap=new char[n];
  memcpy(bitmap,buf+offset,n);
  offset+=n;
  //3.读取field
  for(uint32_t i=0;i<num;i++){
    Field* f;
    TypeId type=schema->GetColumn(i)->GetType();
    bool is_null=(bitmap[i/8]&(0x01<<i%8))==0x00? true:false;
    offset+=Field::DeserializeFrom(buf+offset,type,&f,is_null);
    this->fields_.push_back(f);
  }
  delete[] bitmap;
  return offset;
}

uint32_t Row::GetSerializedSize(Schema *schema) const {
  ASSERT(schema != nullptr, "Invalid schema before serialize.");
  ASSERT(schema->GetColumnCount() == fields_.size(), "Fields size do not match schema's column size.");
  // replace with your code here
  uint32_t num=this->fields_.size();
  uint32_t offset=0;
  if(num==0) return offset;//0直接返回0，不用返回sizeof(uint32_t)
  offset+=sizeof(uint32_t);
  uint32_t n=(num-1)/8*8+1;
  offset+=n;
  for(uint32_t i=0;i<num;i++){
    offset+=this->fields_[i]->GetSerializedSize();
  }
  return offset;
}

void Row::GetKeyFromRow(const Schema *schema, const Schema *key_schema, Row &key_row) {
  auto columns = key_schema->GetColumns();
  std::vector<Field> fields;
  uint32_t idx;
  for (auto column : columns) {
    schema->GetColumnIndex(column->GetName(), idx);
    fields.emplace_back(*this->GetField(idx));
  }
  key_row = Row(fields);
}
