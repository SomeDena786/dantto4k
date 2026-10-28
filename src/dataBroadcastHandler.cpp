#include "dataBroadcastHandler.h"
#include "mhAit.h"
#include "mhSimpleApplicationLocationDescriptor.h"
#include "mhTransportProtocolDescriptor.h"
#include "damt.h"
#include "ddmt.h"
#include "mmtStream.h"
#include "mpu.h"
#include "dataUnit.h"
#include "mmtFragment.h"
#include <tsZlib.h>
#include <cstring>
#include <algorithm>

DataBroadcastHandler& DataBroadcastHandler::instance() {
    static DataBroadcastHandler s;
    return s;
}

void DataBroadcastHandler::setCallback(DataBroadcast4KCallback cb, void* userData) {
    std::lock_guard<std::mutex> lock(mutex_);
    callback_ = cb;
    callbackUserData_ = userData;
}

void DataBroadcastHandler::onMhAit(const MmtTlv::MhAit& mhAit) {
    DataBroadcast4KAppInfo info{};
    info.applicationType = mhAit.applicationType;
    for (auto& app : mhAit.applications) {
        info.organizationId = app.applicationIdentifier.organizationId;
        info.applicationId = app.applicationIdentifier.applicationId;
        info.controlCode = app.applicationControlCode;
        std::string urlBase;
        std::string location;
        for (auto& desc : app.descriptors.list) {
            if (desc->getDescriptorTag() == MmtTlv::MhSimpleApplicationLocationDescriptor::kDescriptorTag) {
                location = static_cast<const MmtTlv::MhSimpleApplicationLocationDescriptor*>(desc.get())->initialPath;
            } else if (desc->getDescriptorTag() == MmtTlv::MhTransportProtocolDescriptor::kDescriptorTag) {
                auto* tp = static_cast<const MmtTlv::MhTransportProtocolDescriptor*>(desc.get());
                // protocol_id=0x0003 (HTTP/HTTPS): selectorにURL_base
                if (tp->protocolId == 0x0003 && !tp->selector.empty()) {
                    size_t len = tp->selector[0];
                    if (len + 1 <= tp->selector.size()) {
                        urlBase.assign(reinterpret_cast<const char*>(tp->selector.data() + 1), len);
                    }
                }
            }
        }
        if (!location.empty()) {
            std::string url = urlBase.empty() ? location : urlBase + location;
            strncpy(info.entryPointUrl, url.c_str(), sizeof(info.entryPointUrl) - 1);
            info.valid = 1;
        }
        // AUTOSTARTを優先する
        if (info.valid && info.controlCode == 0x01) {
            break;
        }
    }
    bool changed;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        changed = memcmp(&aitInfo_, &info, sizeof(info)) != 0;
        aitInfo_ = info;
    }
    if (changed) {
        notify(DataBroadcast4KEventType::AitUpdated);
    }
}

void DataBroadcastHandler::onDdmt(const MmtTlv::Ddmt& ddmt) {
    {
        std::lock_guard<std::mutex> lock(mutex_);
        basePath_ = ddmt.baseDirectoryPath;
        for (auto& node : ddmt.nodes) {
            std::string dir = node.directoryNodePath;
            if (!dir.empty() && dir.back() != '/') {
                dir += '/';
            }
            dirNodeTagToPath_[node.nodeTag] = dir;
            for (auto& file : node.files) {
                nodeTagToPath_[file.nodeTag] = dir + file.fileName;
            }
        }
        rebuildFiles();
    }
    notify(DataBroadcast4KEventType::DirectoryUpdated);
}

void DataBroadcastHandler::onDamt(const MmtTlv::Damt& damt) {
    std::lock_guard<std::mutex> lock(mutex_);
    for (auto& mpu : damt.mpus) {
        DamtMpu m;
        m.indexItemFlag = mpu.indexItemFlag;
        m.indexItemId = mpu.indexItemId;
        m.indexItemCompression = mpu.indexItemCompressionFlag;
        m.directoryNodeTag = mpu.nodeTag;
        for (auto& item : mpu.items) {
            m.nodeTags.push_back(item.nodeTag);
            m.itemIds.push_back(item.itemId);
        }
        damtMpus_[MpuKey{ damt.componentTag, mpu.mpuSequenceNumber }] = std::move(m);
    }
    rebuildFiles();
}

void DataBroadcastHandler::onApplicationData(const MmtTlv::MmtStream& mmtStream, const MmtTlv::Mpu& mpu,
                                              const MmtTlv::DataUnit& dataUnit, const MmtTlv::MfuData& mfuData) {
    ItemKey key{ static_cast<uint16_t>(mmtStream.getComponentTag()), mpu.mpuSequenceNumber, dataUnit.itemId };
    std::vector<uint8_t> complete;
    bool done = false;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        // 非時系列のアイテムはMMTPペイロード単位で分割されて届く
        switch (mpu.fragmentationIndicator) {
        case MmtTlv::FragmentationIndicator::NotFragmented:
            complete = mfuData.data;
            done = true;
            assembling_.erase(key);
            break;
        case MmtTlv::FragmentationIndicator::FirstFragment:
            assembling_[key] = mfuData.data;
            break;
        case MmtTlv::FragmentationIndicator::MiddleFragment: {
            auto it = assembling_.find(key);
            if (it != assembling_.end()) {
                it->second.insert(it->second.end(), mfuData.data.begin(), mfuData.data.end());
            }
            break;
        }
        case MmtTlv::FragmentationIndicator::LastFragment: {
            auto it = assembling_.find(key);
            if (it != assembling_.end()) {
                it->second.insert(it->second.end(), mfuData.data.begin(), mfuData.data.end());
                complete = std::move(it->second);
                assembling_.erase(it);
                done = true;
            }
            break;
        }
        }
        if (!done) {
            return;
        }
        auto prev = items_.find(key);
        if (prev != items_.end() && prev->second == complete) {
            return;
        }
        onItemComplete(key, std::move(complete));
    }
    notify(DataBroadcast4KEventType::FileUpdated);
}

void DataBroadcastHandler::onItemComplete(const ItemKey& key, std::vector<uint8_t>&& data) {
    MpuKey mkey{ key.componentTag, key.mpuSequenceNumber };
    auto damtIt = damtMpus_.find(mkey);
    bool isIndex = false;
    if (damtIt != damtMpus_.end()) {
        isIndex = damtIt->second.indexItemFlag && key.itemId == damtIt->second.indexItemId;
    } else {
        // DAMT未受信: 運用ではindex_item_id=0
        isIndex = key.itemId == 0;
    }
    if (isIndex) {
        std::vector<uint8_t> raw = std::move(data);
        uint8_t compression = damtIt != damtMpus_.end() ? damtIt->second.indexItemCompression : 3;
        if (compression != 3) {
            std::vector<uint8_t> out;
            if (inflate(raw, out, 0)) {
                raw = std::move(out);
            }
        }
        // STD-B60 表10-4
        std::vector<IndexEntry> entries;
        size_t p = 0;
        auto u8 = [&](uint8_t& v) { if (p + 1 > raw.size()) return false; v = raw[p++]; return true; };
        auto u32 = [&](uint32_t& v) { if (p + 4 > raw.size()) return false; v = (raw[p] << 24) | (raw[p + 1] << 16) | (raw[p + 2] << 8) | raw[p + 3]; p += 4; return true; };
        auto str = [&](std::string& s) { uint8_t len; if (!u8(len) || p + len > raw.size()) return false; s.assign(reinterpret_cast<const char*>(raw.data() + p), len); p += len; return true; };
        if (raw.size() >= 2) {
            uint16_t n = (raw[0] << 8) | raw[1];
            p = 2;
            for (uint16_t i = 0; i < n; i++) {
                IndexEntry e{};
                uint8_t flags;
                if (!u32(e.itemId) || !u32(e.itemSize) || !u8(e.itemVersion) || !str(e.fileName) || !u8(flags)) break;
                if (flags & 0x80) {
                    uint32_t checksum;
                    if (!u32(checksum)) break;
                }
                if (!str(e.itemType) || !u8(e.compressionType)) break;
                if (e.compressionType != 0xFF) {
                    if (!u32(e.originalSize)) break;
                }
                entries.push_back(std::move(e));
            }
        }
        indexes_[mkey] = std::move(entries);
    } else {
        items_[key] = std::move(data);
    }
    rebuildFiles();
}

void DataBroadcastHandler::rebuildFiles() {
    files_.clear();
    for (auto& [mkey, m] : damtMpus_) {
        if (m.indexItemFlag) {
            continue;
        }
        for (size_t k = 0; k < m.nodeTags.size() && k < m.itemIds.size(); k++) {
            auto itemIt = items_.find(ItemKey{ mkey.componentTag, mkey.mpuSequenceNumber, m.itemIds[k] });
            auto p = nodeTagToPath_.find(m.nodeTags[k]);
            if (itemIt == items_.end() || p == nodeTagToPath_.end()) {
                continue;
            }
            files_[normalize(p->second)] = File{ std::make_shared<const std::vector<uint8_t>>(itemIt->second), "" };
        }
    }
    for (auto& [mkey, entries] : indexes_) {
        auto damtIt = damtMpus_.find(mkey);
        for (size_t k = 0; k < entries.size(); k++) {
            auto& e = entries[k];
            auto itemIt = items_.find(ItemKey{ mkey.componentTag, mkey.mpuSequenceNumber, e.itemId });
            if (itemIt == items_.end()) {
                continue;
            }
            // パスの決定: DAMTのk番目のnode_tag (ファイルノード) またはMPUに対応付けられたディレクトリ + file_name
            std::vector<std::string> paths;
            if (damtIt != damtMpus_.end()) {
                auto& m = damtIt->second;
                if (k < m.nodeTags.size()) {
                    auto p = nodeTagToPath_.find(m.nodeTags[k]);
                    if (p != nodeTagToPath_.end()) {
                        paths.push_back(p->second);
                    }
                    auto d = dirNodeTagToPath_.find(m.nodeTags[k]);
                    if (d != dirNodeTagToPath_.end()) {
                        paths.push_back(d->second + e.fileName);
                    }
                }
                if (m.directoryNodeTag) {
                    auto d = dirNodeTagToPath_.find(*m.directoryNodeTag);
                    if (d != dirNodeTagToPath_.end()) {
                        paths.push_back(d->second + e.fileName);
                    }
                }
            }
            // 同名ファイルのフォールバック
            for (auto& [tag, path] : nodeTagToPath_) {
                auto slash = path.find_last_of('/');
                if ((slash == std::string::npos ? path : path.substr(slash + 1)) == e.fileName) {
                    paths.push_back(path);
                }
            }
            paths.push_back(e.fileName);
            File f;
            f.contentType = e.itemType;
            std::vector<uint8_t> inflated;
            if (e.compressionType != 0xFF && inflate(itemIt->second, inflated, e.originalSize)) {
                f.data = std::make_shared<const std::vector<uint8_t>>(std::move(inflated));
            } else {
                f.data = std::make_shared<const std::vector<uint8_t>>(itemIt->second);
            }
            for (auto& path : paths) {
                auto n = normalize(path);
                if (!files_.contains(n)) {
                    files_[n] = f;
                }
            }
        }
    }
}

bool DataBroadcastHandler::inflate(const std::vector<uint8_t>& in, std::vector<uint8_t>& out, size_t originalSize) {
    ts::ByteBlock bb;
    if (!ts::Zlib::Decompress(bb, in.data(), in.size())) {
        return false;
    }
    out.assign(bb.begin(), bb.end());
    return originalSize == 0 || out.size() == originalSize || !out.empty();
}

std::string DataBroadcastHandler::normalize(const std::string& path) {
    std::string s = path;
    while (!s.empty() && s[0] == '/') {
        s.erase(0, 1);
    }
    // ファイル名は大文字小文字を区別しない (STD-B62 第4章)
    std::transform(s.begin(), s.end(), s.begin(), [](unsigned char c) { return static_cast<char>(std::tolower(c)); });
    return s;
}

bool DataBroadcastHandler::getApplicationInfo(DataBroadcast4KAppInfo& info) const {
    std::lock_guard<std::mutex> lock(mutex_);
    if (!aitInfo_.valid) return false;
    info = aitInfo_;
    return true;
}

bool DataBroadcastHandler::getFileByPath(const std::string& path, std::vector<uint8_t>& data, std::string* contentType) const {
    std::lock_guard<std::mutex> lock(mutex_);
    auto n = normalize(path);
    auto it = files_.find(n);
    if (it == files_.end()) {
        // base_directory_pathを含む場合
        auto base = normalize(basePath_);
        if (!base.empty() && n.starts_with(base)) {
            it = files_.find(normalize(n.substr(base.size())));
        }
    }
    if (it == files_.end()) return false;
    data = *it->second.data;
    if (contentType) *contentType = it->second.contentType;
    return true;
}

std::string DataBroadcastHandler::getBasePath() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return basePath_;
}

std::vector<std::string> DataBroadcastHandler::listFiles() const {
    std::lock_guard<std::mutex> lock(mutex_);
    std::vector<std::string> r;
    for (auto& [k, v] : files_) r.push_back(k + "\t" + v.contentType + "\t" + std::to_string(v.data->size()));
    std::sort(r.begin(), r.end());
    return r;
}

void DataBroadcastHandler::clear() {
    {
        std::lock_guard<std::mutex> lock(mutex_);
        aitInfo_ = {};
        basePath_.clear();
        nodeTagToPath_.clear();
        dirNodeTagToPath_.clear();
        damtMpus_.clear();
        assembling_.clear();
        items_.clear();
        indexes_.clear();
        files_.clear();
    }
    notify(DataBroadcast4KEventType::ContentReset);
}

void DataBroadcastHandler::notify(DataBroadcast4KEventType event) {
    DataBroadcast4KCallback cb;
    void* ud;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        cb = callback_;
        ud = callbackUserData_;
    }
    if (cb) {
        cb(event, ud);
    }
}

extern "C" {

void __stdcall DB4K_SetCallback(DataBroadcast4KCallback cb, void* userData) {
    DataBroadcastHandler::instance().setCallback(cb, userData);
}

int __stdcall DB4K_GetApplicationInfo(DataBroadcast4KAppInfo* info) {
    if (!info) return 0;
    return DataBroadcastHandler::instance().getApplicationInfo(*info) ? 1 : 0;
}

int __stdcall DB4K_GetFileByPath(const char* path, uint8_t* buffer, uint32_t* size) {
    if (!path || !size) return 0;
    std::vector<uint8_t> data;
    if (!DataBroadcastHandler::instance().getFileByPath(path, data)) return 0;
    if (buffer && *size >= data.size()) {
        memcpy(buffer, data.data(), data.size());
    }
    *size = static_cast<uint32_t>(data.size());
    return 1;
}

int __stdcall DB4K_GetFileContentType(const char* path, char* buffer, uint32_t bufferSize) {
    if (!path || !buffer || bufferSize == 0) return 0;
    std::vector<uint8_t> data;
    std::string type;
    if (!DataBroadcastHandler::instance().getFileByPath(path, data, &type)) return 0;
    strncpy(buffer, type.c_str(), bufferSize - 1);
    buffer[bufferSize - 1] = '\0';
    return 1;
}

int __stdcall DB4K_GetBasePath(char* buffer, uint32_t bufferSize) {
    if (!buffer || bufferSize == 0) return 0;
    auto base = DataBroadcastHandler::instance().getBasePath();
    strncpy(buffer, base.c_str(), bufferSize - 1);
    buffer[bufferSize - 1] = '\0';
    return 1;
}

int __stdcall DB4K_ListFiles(char* buffer, uint32_t* size) {
    if (!size) return 0;
    std::string s;
    for (auto& f : DataBroadcastHandler::instance().listFiles()) s += f + "\n";
    if (buffer && *size > s.size()) {
        memcpy(buffer, s.c_str(), s.size() + 1);
    }
    *size = static_cast<uint32_t>(s.size() + 1);
    return 1;
}

}
