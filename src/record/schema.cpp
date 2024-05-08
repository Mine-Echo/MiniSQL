#include "record/schema.h"

/**
 * TODO: Student Implement
 */
uint32_t Schema::SerializeTo(char *buf) const {
  // replace with your code here
  uint32_t offset=0;
  MACH_WRITE_UINT32(buf,SCHEMA_MAGIC_NUM);
  offset+=sizeof(uint32_t);
  MACH_WRITE_UINT32(buf+offset,this->columns_.size());
  offset+=sizeof(uint32_t);
  //存数组中的内容
  for(int i=0;i<this->columns_.size();i++){
    offset+=this->columns_[i]->SerializeTo(buf+offset);
  }
  return offset;
}

uint32_t Schema::GetSerializedSize() const {
  // replace with your code here
  uint32_t offset=2*sizeof(uint32_t);
  for(int i=0;i<this->columns_.size();i++)
    offset+=columns_[i]->GetSerializedSize();
  return offset;
}

uint32_t Schema::DeserializeFrom(char *buf, Schema *&schema) {
  // replace with your code here
  uint32_t offset=0;
  if(schema!= nullptr){
    LOG(WARNING)<<"Pointer to schema is not null in column deserialize."<<std::endl;
  }
  if((MACH_READ_UINT32(buf)==SCHEMA_MAGIC_NUM)){
    offset+=sizeof(uint32_t);
    uint32_t num= MACH_READ_UINT32(buf+offset);
    offset+=sizeof(uint32_t);
    std::vector<Column*> column(num, nullptr);
    for(int i=0;i<num;i++){
      offset+=column[i]->DeserializeFrom(buf+offset,column[i]);
    }
    schema=new Schema(column);
    return offset;
  }
  LOG(ERROR)<<"The data is not schema."<<std::endl;
  return 0;
}