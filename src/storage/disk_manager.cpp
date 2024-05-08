#include "storage/disk_manager.h"

#include <sys/stat.h>
#include <filesystem>
#include <stdexcept>

#include "glog/logging.h"
#include "page/bitmap_page.h"

DiskManager::DiskManager(const std::string &db_file) : file_name_(db_file) {
  std::scoped_lock<std::recursive_mutex> lock(db_io_latch_);
  db_io_.open(db_file, std::ios::binary | std::ios::in | std::ios::out);
  // directory or file does not exist
  if (!db_io_.is_open()) {
    db_io_.clear();
    // create a new file
    std::filesystem::path p = db_file;
    if(p.has_parent_path()) std::filesystem::create_directories(p.parent_path());
    db_io_.open(db_file, std::ios::binary | std::ios::trunc | std::ios::out);
    db_io_.close();
    // reopen with original mode
    db_io_.open(db_file, std::ios::binary | std::ios::in | std::ios::out);
    if (!db_io_.is_open()) {
      throw std::exception();
    }
  }
  ReadPhysicalPage(META_PAGE_ID, meta_data_);
}

void DiskManager::Close() {
  std::scoped_lock<std::recursive_mutex> lock(db_io_latch_);
  WritePhysicalPage(META_PAGE_ID, meta_data_);
  if (!closed) {
    db_io_.close();
    closed = true;
  }
}

void DiskManager::ReadPage(page_id_t logical_page_id, char *page_data) {
  ASSERT(logical_page_id >= 0, "Invalid page id.");
  ReadPhysicalPage(MapPageId(logical_page_id), page_data);
}

void DiskManager::WritePage(page_id_t logical_page_id, const char *page_data) {
  ASSERT(logical_page_id >= 0, "Invalid page id.");
  WritePhysicalPage(MapPageId(logical_page_id), page_data);
}

/**
 * TODO: Student Implement
 */
page_id_t DiskManager::AllocatePage() {
  //ASSERT(false, "Not implemented yet.");
  DiskFileMetaPage* metaPage=reinterpret_cast<DiskFileMetaPage *>(this->meta_data_);//获得metaPage
  //寻找第一个没有满的分区
  BitmapPage<PAGE_SIZE>* bitmap;
  uint32_t extent=0;
  while(extent<metaPage->num_extents_){
    if(metaPage->extent_used_page_[extent]<BITMAP_SIZE) break;
    extent++;
  }
  if(extent==metaPage->num_extents_){
    if(metaPage->num_extents_<(PAGE_SIZE-8)/4){
      metaPage->num_extents_++;
      metaPage->extent_used_page_[extent]=0;
      //创建位图
      bitmap=new BitmapPage<PAGE_SIZE>();
    }else
      return false;//满了
  }else{//获取位图
    char page_data[PAGE_SIZE];
    ReadPhysicalPage(1+extent*(BITMAP_SIZE+1),page_data);
    bitmap=reinterpret_cast<BitmapPage<PAGE_SIZE>*>(page_data);
  }
  uint32_t offset;
  if(bitmap->AllocatePage(offset))
  {
    //更新metaPage
    metaPage->num_allocated_pages_++;
    metaPage->extent_used_page_[extent]++;
    //if(extent>=metaPage->num_extents_) metaPage->num_extents_++;
    this->WritePhysicalPage(0,this->meta_data_);
    this->WritePhysicalPage(extent*(BITMAP_SIZE+1)+1,reinterpret_cast<char*>(bitmap));
    return extent*BITMAP_SIZE+offset;//返回逻辑地址
  }
  LOG(ERROR) << "AllocatePage wrong" << std::endl;
  return INVALID_PAGE_ID;
}

/**
 * TODO: Student Implement
 */
void DiskManager::DeAllocatePage(page_id_t logical_page_id) {
  uint32_t extent=logical_page_id/BITMAP_SIZE;
  uint32_t offset=logical_page_id%BITMAP_SIZE;
  char page_data[PAGE_SIZE];
  ReadPhysicalPage(1+extent*(BITMAP_SIZE+1),page_data);
  BitmapPage<PAGE_SIZE>* bitmap=reinterpret_cast<BitmapPage<PAGE_SIZE>*>(page_data);
  if(bitmap->DeAllocatePage(offset)){
    DiskFileMetaPage* metaPage=reinterpret_cast<DiskFileMetaPage *>(this->meta_data_);//获得metaPage
    metaPage->num_allocated_pages_--;
    metaPage->extent_used_page_[extent]--;
    if(extent==metaPage->num_extents_-1&&metaPage->extent_used_page_[extent]==0) metaPage->num_extents_--;
    WritePhysicalPage(1+extent*(BITMAP_SIZE+1),reinterpret_cast<char*>(bitmap));
    WritePhysicalPage(0,reinterpret_cast<char*>(metaPage));
  }
  else LOG(WARNING) << "DeAllocatePage warning: the page is free originally" << std::endl;
}

/**
 * TODO: Student Implement
 */
bool DiskManager::IsPageFree(page_id_t logical_page_id) {
  uint32_t extent=logical_page_id/BITMAP_SIZE;
  uint32_t offset=logical_page_id%BITMAP_SIZE;
  char page_data[PAGE_SIZE];
  ReadPhysicalPage(1+extent*(BITMAP_SIZE+1),page_data);
  BitmapPage<PAGE_SIZE>* bitmap=reinterpret_cast<BitmapPage<PAGE_SIZE>*>(page_data);
  return bitmap->IsPageFree(offset);
}

/**
 * TODO: Student Implement
 */
page_id_t DiskManager::MapPageId(page_id_t logical_page_id) {
  uint32_t extent=logical_page_id/BITMAP_SIZE;
  return logical_page_id+1+extent+1;
}

int DiskManager::GetFileSize(const std::string &file_name) {
  struct stat stat_buf;
  int rc = stat(file_name.c_str(), &stat_buf);
  return rc == 0 ? stat_buf.st_size : -1;
}

void DiskManager::ReadPhysicalPage(page_id_t physical_page_id, char *page_data) {
  int offset = physical_page_id * PAGE_SIZE;
  // check if read beyond file length
  if (offset >= GetFileSize(file_name_)) {
#ifdef ENABLE_BPM_DEBUG
    LOG(INFO) << "Read less than a page" << std::endl;
#endif
    memset(page_data, 0, PAGE_SIZE);
  } else {
    // set read cursor to offset
    db_io_.seekp(offset);
    db_io_.read(page_data, PAGE_SIZE);
    // if file ends before reading PAGE_SIZE
    int read_count = db_io_.gcount();
    if (read_count < PAGE_SIZE) {
#ifdef ENABLE_BPM_DEBUG
      LOG(INFO) << "Read less than a page" << std::endl;
#endif
      memset(page_data + read_count, 0, PAGE_SIZE - read_count);
    }
  }
}

void DiskManager::WritePhysicalPage(page_id_t physical_page_id, const char *page_data) {
  size_t offset = static_cast<size_t>(physical_page_id) * PAGE_SIZE;
  // set write cursor to offset
  db_io_.seekp(offset);
  db_io_.write(page_data, PAGE_SIZE);
  // check for I/O error
  if (db_io_.bad()) {
    LOG(ERROR) << "I/O error while writing";
    return;
  }
  // needs to flush to keep disk file in sync
  db_io_.flush();
}