/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include "rm_file_handle.h"

/**
 * @description: 获取当前表中记录号为rid的记录
 * @param {Rid&} rid 记录号，指定记录的位置
 * @param {Context*} context
 * @return {unique_ptr<RmRecord>} rid对应的记录对象指针
 */
std::unique_ptr<RmRecord> RmFileHandle::get_record(const Rid& rid, Context* context) const {
    RmPageHandle page_handle = fetch_page_handle(rid.page_no);
    
    if (!Bitmap::is_set(page_handle.bitmap, rid.slot_no)) {
        buffer_pool_manager_->unpin_page(page_handle.page->get_page_id(), false);
        throw RecordNotFoundError(rid.page_no, rid.slot_no);
    }
    
    char* record_data = page_handle.get_slot(rid.slot_no);
    auto record = std::make_unique<RmRecord>(file_hdr_.record_size, record_data);
    
    buffer_pool_manager_->unpin_page(page_handle.page->get_page_id(), false);
    
    return record;
}

/**
 * @description: 在当前表中插入一条记录，不指定插入位置
 * @param {char*} buf 要插入的记录的数据
 * @param {Context*} context
 * @return {Rid} 插入的记录的记录号（位置）
 */
Rid RmFileHandle::insert_record(char* buf, Context* context) {
    // 1. 获取当前未满的page handle
    RmPageHandle page_handle = create_page_handle();
    
    // 2. 在page handle中找到空闲slot位置
    int slot_no = Bitmap::first_bit(false, page_handle.bitmap, file_hdr_.num_records_per_page);
    if (slot_no < 0) {
        // 这种情况不应该发生，因为create_page_handle应该返回有空闲空间的页面
        throw InternalError("No free slot found in page");
    }
    
    // 3. 将buf复制到空闲slot位置
    char* slot_data = page_handle.get_slot(slot_no);
    memcpy(slot_data, buf, file_hdr_.record_size);
    
    // 4. 更新bitmap和页面头信息
    Bitmap::set(page_handle.bitmap, slot_no);
    page_handle.page_hdr->num_records++;
    
    // 标记页面为脏页
    buffer_pool_manager_->mark_dirty(page_handle.page);
    
    // 检查页面是否已满，如果满了需要从空闲页面链表中移除
    if (page_handle.page_hdr->num_records == file_hdr_.num_records_per_page) {
        // 页面已满，更新file_hdr_.first_free_page_no
        file_hdr_.first_free_page_no = page_handle.page_hdr->next_free_page_no;
        page_handle.page_hdr->next_free_page_no = RM_NO_PAGE;
        
        // 更新文件头到磁盘
        disk_manager_->write_page(fd_, RM_FILE_HDR_PAGE, (char*)&file_hdr_, sizeof(file_hdr_));
    }
    
    // 释放页面句柄
    buffer_pool_manager_->unpin_page(page_handle.page->get_page_id(), true);
    
    return Rid{page_handle.page->get_page_id().page_no, slot_no};
}

/**
 * @description: 在当前表中的指定位置插入一条记录
 * @param {Rid&} rid 要插入记录的位置
 * @param {char*} buf 要插入记录的数据
 */
void RmFileHandle::insert_record(const Rid& rid, char* buf) {
    // 获取指定页面
    RmPageHandle page_handle = fetch_page_handle(rid.page_no);
    
    // 检查该位置是否已被占用
    if (Bitmap::is_set(page_handle.bitmap, rid.slot_no)) {
        buffer_pool_manager_->unpin_page(page_handle.page->get_page_id(), false);
        throw InternalError("Slot is already occupied");
    }
    
    // 插入记录
    char* slot_data = page_handle.get_slot(rid.slot_no);
    memcpy(slot_data, buf, file_hdr_.record_size);
    
    // 更新bitmap和页面头信息
    Bitmap::set(page_handle.bitmap, rid.slot_no);
    page_handle.page_hdr->num_records++;
    
    // 标记页面为脏页
    buffer_pool_manager_->mark_dirty(page_handle.page);
    
    // 检查页面是否已满
    if (page_handle.page_hdr->num_records == file_hdr_.num_records_per_page) {
        // 页面已满，从空闲页面链表中移除
        file_hdr_.first_free_page_no = page_handle.page_hdr->next_free_page_no;
        page_handle.page_hdr->next_free_page_no = RM_NO_PAGE;
        
        // 更新文件头到磁盘
        disk_manager_->write_page(fd_, RM_FILE_HDR_PAGE, (char*)&file_hdr_, sizeof(file_hdr_));
    }
    
    // 释放页面句柄
    buffer_pool_manager_->unpin_page(page_handle.page->get_page_id(), true);
}

/**
 * @description: 删除记录文件中记录号为rid的记录
 * @param {Rid&} rid 要删除的记录的记录号（位置）
 * @param {Context*} context
 */
void RmFileHandle::delete_record(const Rid& rid, Context* context) {
    // 1. 获取指定记录所在的page handle
    RmPageHandle page_handle = fetch_page_handle(rid.page_no);
    
    // 检查记录是否存在
    if (!Bitmap::is_set(page_handle.bitmap, rid.slot_no)) {
        buffer_pool_manager_->unpin_page(page_handle.page->get_page_id(), false);
        throw RecordNotFoundError(rid.page_no,rid.slot_no);
    }
    
    // 记录页面删除前是否已满
    bool was_full = (page_handle.page_hdr->num_records == file_hdr_.num_records_per_page);
    
    // 2. 更新bitmap和页面头信息
    Bitmap::reset(page_handle.bitmap, rid.slot_no);
    page_handle.page_hdr->num_records--;
    
    // 标记页面为脏页
    buffer_pool_manager_->mark_dirty(page_handle.page);
    
    // 3. 如果页面从满变为未满，需要调用release_page_handle()
    if (was_full) {
        release_page_handle(page_handle);
    }
    
    // 释放页面句柄
    buffer_pool_manager_->unpin_page(page_handle.page->get_page_id(), true);
}

/**
 * @description: 更新记录文件中记录号为rid的记录
 * @param {Rid&} rid 要更新的记录的记录号（位置）
 * @param {char*} buf 新记录的数据
 * @param {Context*} context
 */
void RmFileHandle::update_record(const Rid& rid, char* buf, Context* context) {
    // 1. 获取指定记录所在的page handle
    RmPageHandle page_handle = fetch_page_handle(rid.page_no);
    
    // 检查记录是否存在
    if (!Bitmap::is_set(page_handle.bitmap, rid.slot_no)) {
        buffer_pool_manager_->unpin_page(page_handle.page->get_page_id(), false);
        throw RecordNotFoundError(rid.page_no,rid.slot_no);
    }
    
    // 2. 更新记录
    char* slot_data = page_handle.get_slot(rid.slot_no);
    memcpy(slot_data, buf, file_hdr_.record_size);
    
    // 标记页面为脏页
    buffer_pool_manager_->mark_dirty(page_handle.page);
    
    // 释放页面句柄
    buffer_pool_manager_->unpin_page(page_handle.page->get_page_id(), true);
}

/**
 * @description: 获取指定页面的页面句柄
 * @param {int} page_no 页面号
 * @return {RmPageHandle} 指定页面的句柄
 */
// 在关键函数中添加断言和边界检查
RmPageHandle RmFileHandle::fetch_page_handle(int page_no) const {
    if (page_no < 0 || page_no >= file_hdr_.num_pages) {
        throw PageNotExistError("Invalid page number", page_no);
    }
    
    // 检查是否可能的循环引用
    if (page_no == file_hdr_.first_free_page_no) {
        // 额外的验证逻辑
    }
    
    PageId page_id = {fd_, page_no};
    Page* page = buffer_pool_manager_->fetch_page(page_id);
    
    if (page == nullptr) {
        throw InternalError("Failed to fetch page from buffer pool");
    }
    
    return RmPageHandle(&file_hdr_, page);
}

/**
 * @description: 创建一个新的page handle
 * @return {RmPageHandle} 新的PageHandle
 */
RmPageHandle RmFileHandle:: create_new_page_handle() {
    // 1. 使用缓冲池来创建一个新page
    PageId page_id = {fd_, 0}; // page_no会被new_page函数设置
    Page* page = buffer_pool_manager_->new_page(&page_id);
    
    if (page == nullptr) {
        throw InternalError("Failed to create new page");
    }
    
    // 2. 更新page handle中的相关信息
    RmPageHandle page_handle(&file_hdr_, page);
    
    // 初始化页面头
    page_handle.page_hdr->num_records = 0;
    page_handle.page_hdr->next_free_page_no = file_hdr_.first_free_page_no;
    
    // 初始化bitmap为全0（表示所有slot都空闲）
    memset(page_handle.bitmap, 0, file_hdr_.bitmap_size);
    
    // 3. 更新file_hdr_
    file_hdr_.num_pages++;
    file_hdr_.first_free_page_no = page_id.page_no;
    
    // 将更新后的文件头写回磁盘
    disk_manager_->write_page(fd_, RM_FILE_HDR_PAGE, (char*)&file_hdr_, sizeof(file_hdr_));
    
    // 标记新页面为脏页
    buffer_pool_manager_->mark_dirty(page_handle.page);
    
    return page_handle;
}

/**
 * @brief 创建或获取一个空闲的page handle
 *
 * @return RmPageHandle 返回生成的空闲page handle
 * @note pin the page, remember to unpin it outside!
 */
RmPageHandle RmFileHandle::create_page_handle() {
    // 1. 判断file_hdr_中是否还有空闲页
    if (file_hdr_.first_free_page_no == RM_NO_PAGE) {
        // 1.1 没有空闲页：使用缓冲池来创建一个新page
        return create_new_page_handle();
    } else {
        // 1.2 有空闲页：直接获取第一个空闲页
        return fetch_page_handle(file_hdr_.first_free_page_no);
    }
}

/**
 * @description: 当一个页面从没有空闲空间的状态变为有空闲空间状态时，更新文件头和页头中空闲页面相关的元数据
 */
void RmFileHandle::release_page_handle(RmPageHandle& page_handle) {
    page_id_t current_page = page_handle.page->get_page_id().page_no;

    // 检查是否会形成循环引用
    page_id_t check_page = file_hdr_.first_free_page_no;
    while (check_page != RM_NO_PAGE) {
        if (check_page == current_page) {
            return; // 页面已在链表中，避免重复添加
        }

        // 获取下一个空闲页
        PageId pid = {fd_, check_page};
        Page *page = buffer_pool_manager_->fetch_page(pid);
        if (page == nullptr) {
            throw InternalError("Failed to fetch page during release_page_handle");
        }

        auto *page_hdr = reinterpret_cast<RmPageHdr *>(page->get_data());
        check_page = page_hdr->next_free_page_no;

        buffer_pool_manager_->unpin_page(pid, false);
    }

    // 插入到空闲链表头
    page_handle.page_hdr->next_free_page_no = file_hdr_.first_free_page_no;
    file_hdr_.first_free_page_no = current_page;

    // 标记为脏页并更新文件头
    buffer_pool_manager_->mark_dirty(page_handle.page);
    disk_manager_->write_page(fd_, RM_FILE_HDR_PAGE, (char*)&file_hdr_, sizeof(file_hdr_));
}
