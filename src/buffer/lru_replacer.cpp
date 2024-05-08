#include "buffer/lru_replacer.h"

LRUReplacer::LRUReplacer(size_t num_pages){}

LRUReplacer::~LRUReplacer() = default;

/**
 * TODO: Student Implement
 */
//front是最近最少被访问的
bool LRUReplacer::Victim(frame_id_t *frame_id) {
  if(this->Size()==0) return false;//没有可替换的
  *frame_id=this->lru_list_.front();
  this->lru_list_.pop_front();
  this->list_map_.erase(*frame_id);//从hash表中删除
  return true;
}

/**
 * TODO: Student Implement
 */
void LRUReplacer::Pin(frame_id_t frame_id) {
  if(list_map_.find(frame_id)!=list_map_.end())//找到了，把它删除；没找到不用管
  {
    this->lru_list_.erase(this->list_map_[frame_id]);
    this->list_map_.erase(frame_id);
  }
}

/**
 * TODO: Student Implement
 */
void LRUReplacer::Unpin(frame_id_t frame_id) {
  //if(list_map_.find(frame_id)!=list_map_.end()) lru_list_.erase(list_map_[frame_id]);//存在则先删除
  if(list_map_.find(frame_id)==list_map_.end()) {
    this->lru_list_.push_back(frame_id);      // 插入末尾
    list_map_[frame_id] = --lru_list_.end();  // 插入map
  }
}

/**
 * TODO: Student Implement
 */
size_t LRUReplacer::Size() {
  return this->lru_list_.size();
}