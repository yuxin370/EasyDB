/*-------------------------------------------------------------------------
 *
 * EasyDB
 *
 * config.h
 *
 * Identification: src/include/common/config.h
 *
 *-------------------------------------------------------------------------
 */

/*
 * Original copyright:
 * Copyright (c) 2015-2019, Carnegie Mellon University Database Group
 */

#pragma once

#include <atomic>
#include <chrono>
#include <cstdint>
#include <string>
namespace easydb {

/** Cycle detection is performed every CYCLE_DETECTION_INTERVAL milliseconds. */
extern std::chrono::milliseconds cycle_detection_interval;

/** True if logging should be enabled, false otherwise. */
extern std::atomic<bool> enable_logging;

/** If ENABLE_LOGGING is true, the log should be flushed to disk every LOG_TIMEOUT. */
extern std::chrono::duration<int64_t> log_timeout;

static constexpr int INVALID_FRAME_ID = -1;  // invalid frame id
static constexpr int INVALID_PAGE_ID = -1;   // invalid page id
static constexpr int INVALID_TXN_ID = -1;    // invalid transaction id
static constexpr int INVALID_LSN = -1;       // invalid log sequence number

static constexpr int PAGE_SIZE = 4096;                                        // size of a data page in byte
static constexpr int BUFFER_POOL_SIZE = 1024;                                 // size of buffer pool
static constexpr int DEFAULT_DB_IO_SIZE = 16;                                 // starting size of file on disk
static constexpr int LOG_BUFFER_SIZE = ((BUFFER_POOL_SIZE + 1) * PAGE_SIZE);  // size of a log buffer in byte
static constexpr int BUCKET_SIZE = 64;                                        // size of extendible hash bucket
// static constexpr int LRUK_REPLACER_K = 10;                                    // backward k-distance for lru-k

using frame_id_t = int32_t;    // frame id type
using page_id_t = int32_t;     // page id type
using slot_id_t = uint32_t;    // slot id type
using txn_id_t = int64_t;      // transaction id type
using lsn_t = int32_t;         // log sequence number type
using slot_offset_t = size_t;  // slot offset type
using oid_t = uint16_t;

const txn_id_t TXN_START_ID = 1LL << 62;  // first txn id

static constexpr int VARCHAR_DEFAULT_LENGTH = 128;  // default length for varchar when constructing the column

// log file
static const std::string LOG_FILE_NAME = "db.log";
static const std::string RESTART_FILE_NAME = "db.restart";

// replacer
static const std::string REPLACER_TYPE = "LRU";
static const std::string DB_META_NAME = "db.meta";

static const std::string DB_NAME = "test.db";

static constexpr int BUFFER_LENGTH = 8192;

}  // namespace easydb
