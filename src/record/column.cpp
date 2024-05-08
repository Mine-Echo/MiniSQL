#include "record/column.h"

#include "glog/logging.h"

Column::Column(std::string column_name, TypeId type, uint32_t index, bool nullable, bool unique)
    : name_(std::move(column_name)), type_(type), table_ind_(index), nullable_(nullable), unique_(unique) {
  ASSERT(type != TypeId::kTypeChar, "Wrong constructor for CHAR type.");
  switch (type) {
    case TypeId::kTypeInt:
      len_ = sizeof(int32_t);
      break;
    case TypeId::kTypeFloat:
      len_ = sizeof(float_t);
      break;
    default:
      ASSERT(false, "Unsupported column type.");
  }
}

Column::Column(std::string column_name, TypeId type, uint32_t length, uint32_t index, bool nullable, bool unique)
    : name_(std::move(column_name)),
      type_(type),
      len_(length),
      table_ind_(index),
      nullable_(nullable),
      unique_(unique) {
  ASSERT(type == TypeId::kTypeChar, "Wrong constructor for non-VARCHAR type.");
}

Column::Column(const Column *other)
    : name_(other->name_),
      type_(other->type_),
      len_(other->len_),
      table_ind_(other->table_ind_),
      nullable_(other->nullable_),
      unique_(other->unique_) {}

/**
* TODO: Student Implement
*/
uint32_t Column::SerializeTo(char *buf) const {
  // replace with your code here
  uint32_t offset=0;
  //1.MAGIC_NUM
  MACH_WRITE_UINT32(buf,COLUMN_MAGIC_NUM);
  offset+=sizeof(uint32_t);
  //2.name_len
  MACH_WRITE_UINT32(buf+offset,this->name_.size());
  offset+=sizeof(uint32_t);
  //3.name
  MACH_WRITE_STRING(buf+offset,this->name_);
  offset+=this->name_.size();
  //4.type_id
  MACH_WRITE_TO(TypeId,buf+offset,this->type_);
  offset+=sizeof(TypeId);
  //5.len
  MACH_WRITE_UINT32(buf+offset,this->len_);
  offset+=sizeof(uint32_t);
  //6.table_int
  MACH_WRITE_UINT32(buf+offset,this->table_ind_);
  offset+=sizeof(uint32_t);
  //7.nullable
  MACH_WRITE_TO(bool,buf+offset,this->nullable_);
  offset+=sizeof(bool);
  //8.unique
  MACH_WRITE_TO(bool,buf+offset,this->unique_);
  offset+=sizeof(bool);
  return offset;
}

/**
 * TODO: Student Implement
 */
uint32_t Column::GetSerializedSize() const {
  // replace with your code here
  return 4*sizeof(uint32_t)+this->name_.size()+2*sizeof(bool)+sizeof(TypeId);
}

/**
 * TODO: Student Implement
 */
uint32_t Column::DeserializeFrom(char *buf, Column *&column) {
  // replace with your code here
  uint32_t offset=0;
  if(column== nullptr) {
    LOG(WARNING)<<"Pointer to column is not null in column deserialize."<<std::endl;
  }
  if((MACH_READ_UINT32(buf+offset))==COLUMN_MAGIC_NUM){
    offset+=sizeof(uint32_t);
    //2.name_len
    uint32_t name_len= MACH_READ_UINT32(buf+offset);
    offset+=sizeof(uint32_t);
    //3.name
    char *name=new char[name_len+1];
    name[name_len]=0;
    memcpy(name,buf+offset,name_len);
    offset+=name_len;
    //4.type_id
    TypeId type_id= MACH_READ_FROM(TypeId,buf+offset);
    offset+=sizeof(TypeId);
    //5.len
    uint32_t len= MACH_READ_UINT32(buf+offset);
    offset+=sizeof(uint32_t);
    //6.table_int
    uint32_t table_int= MACH_READ_UINT32(buf+offset);
    offset+=sizeof(uint32_t);
    //7.nullable
    bool nullable= MACH_READ_FROM(bool,buf+offset);
    offset+=sizeof(bool);
    //7.unique
    bool unique= MACH_READ_FROM(bool,buf+offset);
    offset+=sizeof(bool);
    //column=new Column(std::string(name),type_id,len,table_int,nullable,unique);
    if(type_id==TypeId::kTypeChar)
      column=new Column(std::string(name),type_id,len,table_int,nullable,unique);
    else
      column=new Column(std::string(name),type_id,table_int,nullable,unique);
    delete[] name;
    return offset;
  }
  LOG(ERROR)<<"The data is not column."<<std::endl;
  return 0;
}
