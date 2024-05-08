#include "page/bitmap_page.h"

#include "glog/logging.h"

/**
 * TODO: Student Implement
 */
template <size_t PageSize>
bool BitmapPage<PageSize>::AllocatePage(uint32_t &page_offset) {
  if(page_allocated_<this->GetMaxSupportedSize()){
    page_offset=next_free_page_;//将空闲数据页的序号给offset
    page_allocated_++;
    //将标记设置为1
    this->bytes[page_offset/8]|=((unsigned char)0x80)>>(page_offset%8);
    //重新获得next_free_page_,从0开始找
    next_free_page_=0;
    while(next_free_page_<this->GetMaxSupportedSize()){
      if(IsPageFreeLow(next_free_page_/8,next_free_page_%8)) break;
      next_free_page_++;
    }
    return true;
  }
  return false;
}

/**
 * TODO: Student Implement
 */
template <size_t PageSize>
bool BitmapPage<PageSize>::DeAllocatePage(uint32_t page_offset) {
  if(!this->IsPageFreeLow(page_offset/8,page_offset%8)){
    page_allocated_--;
    next_free_page_=page_offset;//如果不满也可以不执行
//    if(next_free_page_>=this->GetMaxSupportedSize())
//      next_free_page_=page_offset;
    //将标记设置为0
    this->bytes[page_offset/8]&=~(((unsigned char)0x80)>>(page_offset%8));
    return true;
  }
  return false;
}

/**
 * TODO: Student Implement
 */
template <size_t PageSize>
bool BitmapPage<PageSize>::IsPageFree(uint32_t page_offset) const {
  return this->IsPageFreeLow(page_offset/8,page_offset%8);
}

template <size_t PageSize>
bool BitmapPage<PageSize>::IsPageFreeLow(uint32_t byte_index, uint8_t bit_index) const {
  return ((this->bytes[byte_index]>>(7-bit_index))%2==0? true: false);
}

template class BitmapPage<64>;

template class BitmapPage<128>;

template class BitmapPage<256>;

template class BitmapPage<512>;

template class BitmapPage<1024>;

template class BitmapPage<2048>;

template class BitmapPage<4096>;