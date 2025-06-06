/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include "rm_scan.h"
#include "rm_file_handle.h"

/**
 * @brief 初始化file_handle和rid
 * @param file_handle
 */
RmScan::RmScan(const RmFileHandle *file_handle) : file_handle_(file_handle) {
    // Todo:
    // 初始化file_handle和rid（指向第一个存放了记录的位置）
    rid_ = Rid{-1, -1};  // 初始设置为无效位置
    next();              // 自动跳到第一个非空slot
}

/**
 * @brief 找到文件中下一个存放了记录的位置
 */
void RmScan::next() {
    int page_no = rid_.page_no;
    int slot_no = rid_.slot_no + 1;

    if (page_no == -1) {
        page_no = 0;
        slot_no = 0;
    }

    const int total_pages = file_handle_->file_hdr_.num_pages;
    const int slots_per_page = file_handle_->file_hdr_.num_records_per_page;

    while (page_no < total_pages) {
        RmPageHandle page_handle = file_handle_->fetch_page_handle(page_no);

        while (slot_no < slots_per_page) {
            if (Bitmap::is_set(page_handle.bitmap, slot_no)) {
                rid_ = Rid{page_no, slot_no};

                file_handle_->buffer_pool_manager_->unpin_page(page_handle.page->get_page_id(), false);
                return;
            }
            slot_no++;
        }

        file_handle_->buffer_pool_manager_->unpin_page(page_handle.page->get_page_id(), false);
        page_no++;
        slot_no = 0;
    }

    rid_ = Rid{-1, -1};  // end of scan
}


/**
 * @brief ​ 判断是否到达文件末尾
 */
bool RmScan::is_end() const {
    // Todo: 修改返回值

    return rid_.page_no == -1;
}

/**
 * @brief RmScan内部存放的rid
 */
Rid RmScan::rid() const {
    return rid_;
}