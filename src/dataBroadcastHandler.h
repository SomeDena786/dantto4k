#pragma once
// BS4K データ放送 (ARIB-HTML5, STD-B60 第10章 アプリケーション伝送方式)
// MMTで伝送されるMH-AIT/DDMT/DAMTとアプリケーションのアイテム(ファイル)を保持し
// TVTestプラグインからDB4K_*で参照できるようにする
#include "demuxerHandler.h"
#include <mutex>
#include <map>
#include <unordered_map>
#include <string>
#include <vector>
#include <optional>
#include <tuple>
#include <memory>

namespace MmtTlv {
class MmtTlvDemuxer;
}

struct DataBroadcast4KAppInfo {
    uint16_t applicationType;
    uint32_t organizationId;
    uint16_t applicationId;
    uint8_t controlCode;
    char entryPointUrl[512];
    int valid;
};

enum class DataBroadcast4KEventType {
    AitUpdated,
    DirectoryUpdated,
    FileUpdated,
    ContentReset,
};

using DataBroadcast4KCallback = void(*)(DataBroadcast4KEventType event, void* userData);

class DataBroadcastHandler {
public:
    static DataBroadcastHandler& instance();

    void onMhAit(const MmtTlv::MhAit& mhAit);
    void onDdmt(const MmtTlv::Ddmt& ddmt);
    void onDamt(const MmtTlv::Damt& damt);
    void onApplicationData(const MmtTlv::MmtStream& mmtStream, const MmtTlv::Mpu& mpu,
                           const MmtTlv::DataUnit& dataUnit, const MmtTlv::MfuData& mfuData);

    void setCallback(DataBroadcast4KCallback cb, void* userData);
    bool getApplicationInfo(DataBroadcast4KAppInfo& info) const;
    // directory_node_path + file_name (base_directory_pathを含めてもよい)
    bool getFileByPath(const std::string& path, std::vector<uint8_t>& data, std::string* contentType = nullptr) const;
    std::string getBasePath() const;
    std::vector<std::string> listFiles() const;
    void clear();

private:
    DataBroadcastHandler() = default;

    struct IndexEntry {
        uint32_t itemId;
        uint32_t itemSize;
        uint8_t itemVersion;
        std::string fileName;
        std::string itemType;
        uint8_t compressionType;
        uint32_t originalSize;
    };
    struct ItemKey {
        uint16_t componentTag;
        uint32_t mpuSequenceNumber;
        uint32_t itemId;
        bool operator<(const ItemKey& o) const {
            return std::tie(componentTag, mpuSequenceNumber, itemId) < std::tie(o.componentTag, o.mpuSequenceNumber, o.itemId);
        }
    };
    struct MpuKey {
        uint16_t componentTag;
        uint32_t mpuSequenceNumber;
        bool operator<(const MpuKey& o) const {
            return std::tie(componentTag, mpuSequenceNumber) < std::tie(o.componentTag, o.mpuSequenceNumber);
        }
    };
    struct DamtMpu {
        uint8_t indexItemCompression = 3;
        uint32_t indexItemId = 0;
        bool indexItemFlag = false;
        std::vector<uint16_t> nodeTags;
        std::vector<uint32_t> itemIds; // index_item_flag==0 の場合
        std::optional<uint16_t> directoryNodeTag;
    };
    struct File {
        std::shared_ptr<const std::vector<uint8_t>> data;
        std::string contentType;
    };

    void onItemComplete(const ItemKey& key, std::vector<uint8_t>&& data);
    void rebuildFiles();
    void notify(DataBroadcast4KEventType event);
    static bool inflate(const std::vector<uint8_t>& in, std::vector<uint8_t>& out, size_t originalSize);
    static std::string normalize(const std::string& path);

    mutable std::mutex mutex_;
    DataBroadcast4KCallback callback_ = nullptr;
    void* callbackUserData_ = nullptr;

    DataBroadcast4KAppInfo aitInfo_{};
    // DDMT
    std::string basePath_;
    std::unordered_map<uint16_t, std::string> nodeTagToPath_;   // ファイルノード → フルパス
    std::unordered_map<uint16_t, std::string> dirNodeTagToPath_; // ディレクトリノード → パス
    // DAMT
    std::map<MpuKey, DamtMpu> damtMpus_;
    // 受信したアイテム
    std::map<ItemKey, std::vector<uint8_t>> assembling_;
    std::map<ItemKey, std::vector<uint8_t>> items_;
    std::map<MpuKey, std::vector<IndexEntry>> indexes_;
    // パス → ファイル
    std::unordered_map<std::string, File> files_;
};

extern "C" {
    __declspec(dllexport) void __stdcall DB4K_SetCallback(DataBroadcast4KCallback cb, void* userData);
    __declspec(dllexport) int __stdcall DB4K_GetApplicationInfo(DataBroadcast4KAppInfo* info);
    __declspec(dllexport) int __stdcall DB4K_GetFileByPath(const char* path, uint8_t* buffer, uint32_t* size);
    // Content-Typeを取得 (インデックスアイテムのitem_type)
    __declspec(dllexport) int __stdcall DB4K_GetFileContentType(const char* path, char* buffer, uint32_t bufferSize);
    __declspec(dllexport) int __stdcall DB4K_GetBasePath(char* buffer, uint32_t bufferSize);
    // デバッグ用: 改行区切りのパス一覧
    __declspec(dllexport) int __stdcall DB4K_ListFiles(char* buffer, uint32_t* size);
}
