#pragma once

#include <cstdint>
#include <unordered_map>
#include <vector>
#include <optional>
#include <array>
#include <bitset>
#include <algorithm>
#include <functional>
#include <filesystem>
#include <fstream>
#include <concepts>
#include <span>

namespace voxel_crdt {

// Phase 2: Compression, Spatial Queries, Bulk Operations, Abstract Storage
// Focus: performance optimizations, flexible persistence, spatial operations

using NodeId = uint64_t;
using VoxelData = uint16_t; // material ID + flags
using ChunkSize = std::integral_constant<int, 32>; // 32x32x32 = 32K voxels per chunk

struct VoxelCoord {
    int32_t x, y, z;
    
    constexpr VoxelCoord(int32_t x = 0, int32_t y = 0, int32_t z = 0) : x(x), y(y), z(z) {}
    
    constexpr bool operator==(const VoxelCoord& other) const {
        return x == other.x && y == other.y && z == other.z;
    }
    
    constexpr VoxelCoord operator+(const VoxelCoord& other) const {
        return {x + other.x, y + other.y, z + other.z};
    }
    
    constexpr VoxelCoord operator-(const VoxelCoord& other) const {
        return {x - other.x, y - other.y, z - other.z};
    }
};

struct ChunkCoord {
    int32_t x, y, z;
    
    constexpr ChunkCoord(int32_t x = 0, int32_t y = 0, int32_t z = 0) : x(x), y(y), z(z) {}
    
    constexpr bool operator==(const ChunkCoord& other) const {
        return x == other.x && y == other.y && z == other.z;
    }
    
    // Convert world voxel coordinate to chunk coordinate
    static constexpr ChunkCoord from_voxel(const VoxelCoord& voxel) {
        return {
            voxel.x >> 5, // divide by 32
            voxel.y >> 5,
            voxel.z >> 5
        };
    }
    
    // Get all 26 neighboring chunk coordinates
    std::vector<ChunkCoord> neighbors() const {
        std::vector<ChunkCoord> result;
        result.reserve(26);
        for (int dx = -1; dx <= 1; ++dx) {
            for (int dy = -1; dy <= 1; ++dy) {
                for (int dz = -1; dz <= 1; ++dz) {
                    if (dx == 0 && dy == 0 && dz == 0) continue;
                    result.emplace_back(x + dx, y + dy, z + dz);
                }
            }
        }
        return result;
    }
};

struct BoundingBox {
    VoxelCoord min, max;
    
    BoundingBox(VoxelCoord min, VoxelCoord max) : min(min), max(max) {}
    
    bool contains(const VoxelCoord& coord) const {
        return coord.x >= min.x && coord.x <= max.x &&
               coord.y >= min.y && coord.y <= max.y &&
               coord.z >= min.z && coord.z <= max.z;
    }
    
    // Get all chunk coordinates that intersect this bounding box
    std::vector<ChunkCoord> intersecting_chunks() const {
        ChunkCoord min_chunk = ChunkCoord::from_voxel(min);
        ChunkCoord max_chunk = ChunkCoord::from_voxel(max);
        
        std::vector<ChunkCoord> chunks;
        for (int x = min_chunk.x; x <= max_chunk.x; ++x) {
            for (int y = min_chunk.y; y <= max_chunk.y; ++y) {
                for (int z = min_chunk.z; z <= max_chunk.z; ++z) {
                    chunks.emplace_back(x, y, z);
                }
            }
        }
        return chunks;
    }
};

// Hash functions
struct VoxelCoordHash {
    std::size_t operator()(const VoxelCoord& coord) const {
        return std::hash<uint64_t>{}(
            (static_cast<uint64_t>(coord.x) << 32) |
            (static_cast<uint64_t>(coord.y) << 16) |
            static_cast<uint64_t>(coord.z)
        );
    }
};

struct ChunkCoordHash {
    std::size_t operator()(const ChunkCoord& coord) const {
        return std::hash<uint64_t>{}(
            (static_cast<uint64_t>(coord.x) << 32) |
            (static_cast<uint64_t>(coord.y) << 16) |
            static_cast<uint64_t>(coord.z)
        );
    }
};

struct VoxelChange {
    VoxelCoord coord;
    std::optional<VoxelData> value; // nullopt = deletion
    uint64_t version;
    NodeId node_id;
    
    VoxelChange(VoxelCoord coord, std::optional<VoxelData> value, uint64_t version, NodeId node_id)
        : coord(coord), value(value), version(version), node_id(node_id) {}
};

// Compact version tracking per voxel within chunk
struct VoxelVersion {
    uint32_t version : 24;  // 16M versions should be enough per voxel
    uint32_t node_id : 8;   // 256 concurrent nodes max
    
    VoxelVersion(uint32_t v = 0, uint32_t n = 0) : version(v & 0xFFFFFF), node_id(n & 0xFF) {}
    
    bool should_accept(const VoxelVersion& remote) const {
        return remote.version > version || 
               (remote.version == version && remote.node_id > node_id);
    }
};

// RLE compression for contiguous regions of same material
struct RLESegment {
    VoxelData value;
    uint16_t run_length;  // up to 65K consecutive voxels
    
    RLESegment(VoxelData val, uint16_t len) : value(val), run_length(len) {}
};

// Forward declaration
class VoxelChunk;

// Storage backend concept - can be SQLite, files, network, etc.
template<typename T>
concept VoxelStorage = requires(T storage, const ChunkCoord& coord, const VoxelChunk& chunk) {
    { storage.save_chunk(coord, chunk) } -> std::same_as<bool>;
    { storage.load_chunk(coord) } -> std::same_as<std::optional<VoxelChunk>>;
    { storage.delete_chunk(coord) } -> std::same_as<bool>;
    { storage.list_chunks() } -> std::same_as<std::vector<ChunkCoord>>;
    { storage.chunk_exists(coord) } -> std::same_as<bool>;
};

class VoxelChunk {
    static constexpr size_t CHUNK_VOLUME = ChunkSize::value * ChunkSize::value * ChunkSize::value;
    
    std::array<VoxelData, CHUNK_VOLUME> data_{};
    std::array<VoxelVersion, CHUNK_VOLUME> versions_{};
    std::bitset<CHUNK_VOLUME> active_{}; // track which voxels are set
    
    uint64_t chunk_version_ = 0;
    bool compressed_ = false;
    std::vector<RLESegment> rle_data_; // compressed representation
    
    static constexpr size_t coord_to_index(int x, int y, int z) {
        return (z * ChunkSize::value + y) * ChunkSize::value + x;
    }
    
    static constexpr size_t local_coord_to_index(const VoxelCoord& local) {
        return coord_to_index(local.x & 31, local.y & 31, local.z & 31);
    }
    
    void decompress() {
        if (!compressed_) return;
        
        size_t index = 0;
        for (const auto& segment : rle_data_) {
            for (uint16_t i = 0; i < segment.run_length && index < CHUNK_VOLUME; ++i, ++index) {
                if (segment.value != 0) {
                    data_[index] = segment.value;
                    active_[index] = true;
                } else {
                    active_[index] = false;
                }
            }
        }
        compressed_ = false;
        rle_data_.clear();
    }
    
    void compress() {
        if (compressed_) return;
        
        rle_data_.clear();
        if (active_.none()) {
            compressed_ = true;
            return; // Empty chunk compresses to nothing
        }
        
        VoxelData current_value = active_[0] ? data_[0] : 0;
        uint16_t run_length = 1;
        
        for (size_t i = 1; i < CHUNK_VOLUME; ++i) {
            VoxelData value = active_[i] ? data_[i] : 0;
            
            if (value == current_value && run_length < UINT16_MAX) {
                ++run_length;
            } else {
                rle_data_.emplace_back(current_value, run_length);
                current_value = value;
                run_length = 1;
            }
        }
        
        // Add final segment
        rle_data_.emplace_back(current_value, run_length);
        compressed_ = true;
    }

public:
    std::optional<VoxelData> get_voxel(const VoxelCoord& local_coord) const {
        const_cast<VoxelChunk*>(this)->decompress(); // Lazy decompression
        size_t idx = local_coord_to_index(local_coord);
        return active_[idx] ? std::optional{data_[idx]} : std::nullopt;
    }
    
    bool set_voxel(const VoxelCoord& local_coord, std::optional<VoxelData> value, 
                   uint64_t version, NodeId node_id) {
        decompress(); // Ensure we're working with uncompressed data
        
        size_t idx = local_coord_to_index(local_coord);
        VoxelVersion new_version(version, node_id);
        
        if (!active_[idx] || versions_[idx].should_accept(new_version)) {
            if (value.has_value()) {
                data_[idx] = *value;
                active_[idx] = true;
            } else {
                active_[idx] = false;
            }
            versions_[idx] = new_version;
            chunk_version_ = std::max(chunk_version_, version);
            return true;
        }
        return false;
    }
    
    // Bulk operation: fill a region with same material
    size_t fill_region(const VoxelCoord& start, const VoxelCoord& end, VoxelData value, 
                       uint64_t version, NodeId node_id) {
        decompress();
        
        size_t changes = 0;
        VoxelVersion new_version(version, node_id);
        
        for (int x = std::max(0, start.x); x <= std::min(31, end.x); ++x) {
            for (int y = std::max(0, start.y); y <= std::min(31, end.y); ++y) {
                for (int z = std::max(0, start.z); z <= std::min(31, end.z); ++z) {
                    size_t idx = coord_to_index(x, y, z);
                    if (!active_[idx] || versions_[idx].should_accept(new_version)) {
                        data_[idx] = value;
                        active_[idx] = true;
                        versions_[idx] = new_version;
                        chunk_version_ = std::max(chunk_version_, version);
                        ++changes;
                    }
                }
            }
        }
        
        return changes;
    }
    
    // Get all voxels in a region (for spatial queries)
    std::vector<std::pair<VoxelCoord, VoxelData>> get_region(const VoxelCoord& start, const VoxelCoord& end) const {
        const_cast<VoxelChunk*>(this)->decompress();
        
        std::vector<std::pair<VoxelCoord, VoxelData>> result;
        
        for (int x = std::max(0, start.x); x <= std::min(31, end.x); ++x) {
            for (int y = std::max(0, start.y); y <= std::min(31, end.y); ++y) {
                for (int z = std::max(0, start.z); z <= std::min(31, end.z); ++z) {
                    size_t idx = coord_to_index(x, y, z);
                    if (active_[idx]) {
                        result.emplace_back(VoxelCoord{x, y, z}, data_[idx]);
                    }
                }
            }
        }
        
        return result;
    }
    
    uint64_t get_chunk_version() const { return chunk_version_; }
    
    std::vector<VoxelChange> get_changes_since(const ChunkCoord& chunk_coord, uint64_t since_version) const {
        const_cast<VoxelChunk*>(this)->decompress();
        
        std::vector<VoxelChange> changes;
        
        for (int z = 0; z < ChunkSize::value; ++z) {
            for (int y = 0; y < ChunkSize::value; ++y) {
                for (int x = 0; x < ChunkSize::value; ++x) {
                    size_t idx = coord_to_index(x, y, z);
                    if (versions_[idx].version > since_version) {
                        VoxelCoord world_coord{
                            chunk_coord.x * ChunkSize::value + x,
                            chunk_coord.y * ChunkSize::value + y,
                            chunk_coord.z * ChunkSize::value + z
                        };
                        
                        std::optional<VoxelData> value = active_[idx] ? 
                            std::optional{data_[idx]} : std::nullopt;
                        
                        changes.emplace_back(world_coord, value, versions_[idx].version, versions_[idx].node_id);
                    }
                }
            }
        }
        
        return changes;
    }
    
    size_t active_voxel_count() const {
        return active_.count();
    }
    
    bool is_empty() const {
        return active_.none();
    }
    
    // Compression interface
    void force_compress() { compress(); }
    bool is_compressed() const { return compressed_; }
    size_t compressed_size() const { return rle_data_.size(); }
    
    // Serialization support
    std::vector<uint8_t> serialize() const;
    static std::optional<VoxelChunk> deserialize(const std::vector<uint8_t>& data);
    
    friend class FileStorage; // For direct access during serialization
};

// Simple file-based storage backend for testing
class FileStorage {
    std::filesystem::path base_path_;
    
    std::string chunk_filename(const ChunkCoord& coord) const {
        return base_path_ / ("chunk_" + std::to_string(coord.x) + "_" + 
                            std::to_string(coord.y) + "_" + std::to_string(coord.z) + ".voxel");
    }
    
public:
    explicit FileStorage(const std::filesystem::path& path) : base_path_(path) {
        std::filesystem::create_directories(base_path_);
    }
    
    bool save_chunk(const ChunkCoord& coord, const VoxelChunk& chunk) {
        try {
            auto data = chunk.serialize();
            std::ofstream file(chunk_filename(coord), std::ios::binary);
            file.write(reinterpret_cast<const char*>(data.data()), data.size());
            return file.good();
        } catch (...) {
            return false;
        }
    }
    
    std::optional<VoxelChunk> load_chunk(const ChunkCoord& coord) {
        try {
            std::ifstream file(chunk_filename(coord), std::ios::binary);
            if (!file.good()) return std::nullopt;
            
            file.seekg(0, std::ios::end);
            size_t size = file.tellg();
            file.seekg(0, std::ios::beg);
            
            std::vector<uint8_t> data(size);
            file.read(reinterpret_cast<char*>(data.data()), size);
            
            return VoxelChunk::deserialize(data);
        } catch (...) {
            return std::nullopt;
        }
    }
    
    bool delete_chunk(const ChunkCoord& coord) {
        try {
            return std::filesystem::remove(chunk_filename(coord));
        } catch (...) {
            return false;
        }
    }
    
    bool chunk_exists(const ChunkCoord& coord) {
        return std::filesystem::exists(chunk_filename(coord));
    }
    
    std::vector<ChunkCoord> list_chunks() {
        std::vector<ChunkCoord> chunks;
        
        try {
            for (const auto& entry : std::filesystem::directory_iterator(base_path_)) {
                if (entry.is_regular_file() && entry.path().extension() == ".voxel") {
                    std::string filename = entry.path().stem().string();
                    if (filename.starts_with("chunk_")) {
                        // Parse "chunk_x_y_z"
                        auto parts = filename.substr(6); // Remove "chunk_"
                        size_t pos1 = parts.find('_');
                        size_t pos2 = parts.find('_', pos1 + 1);
                        
                        if (pos1 != std::string::npos && pos2 != std::string::npos) {
                            int x = std::stoi(parts.substr(0, pos1));
                            int y = std::stoi(parts.substr(pos1 + 1, pos2 - pos1 - 1));
                            int z = std::stoi(parts.substr(pos2 + 1));
                            chunks.emplace_back(x, y, z);
                        }
                    }
                }
            }
        } catch (...) {
            // Return partial results on error
        }
        
        return chunks;
    }
};

template<VoxelStorage StorageBackend>
class VoxelCRDT {
    std::unordered_map<ChunkCoord, VoxelChunk, ChunkCoordHash> chunks_;
    StorageBackend storage_;
    NodeId node_id_;
    uint64_t current_version_ = 0;
    
    VoxelChunk& load_or_create_chunk(const ChunkCoord& coord) {
        auto it = chunks_.find(coord);
        if (it != chunks_.end()) {
            return it->second;
        }
        
        // Try loading from storage
        if (auto loaded = storage_.load_chunk(coord)) {
            auto [inserted_it, success] = chunks_.emplace(coord, std::move(*loaded));
            return inserted_it->second;
        }
        
        // Create new chunk
        auto [inserted_it, success] = chunks_.emplace(coord, VoxelChunk{});
        return inserted_it->second;
    }
    
public:
    explicit VoxelCRDT(NodeId node_id, StorageBackend storage = StorageBackend{}) 
        : storage_(std::move(storage)), node_id_(node_id) {}
    
    void set_voxel(const VoxelCoord& coord, VoxelData value) {
        ChunkCoord chunk_coord = ChunkCoord::from_voxel(coord);
        VoxelChunk& chunk = load_or_create_chunk(chunk_coord);
        
        ++current_version_;
        if (chunk.set_voxel(coord, value, current_version_, node_id_)) {
            storage_.save_chunk(chunk_coord, chunk); // Auto-persist
        }
    }
    
    void remove_voxel(const VoxelCoord& coord) {
        ChunkCoord chunk_coord = ChunkCoord::from_voxel(coord);
        VoxelChunk& chunk = load_or_create_chunk(chunk_coord);
        
        ++current_version_;
        if (chunk.set_voxel(coord, std::nullopt, current_version_, node_id_)) {
            if (chunk.is_empty()) {
                chunks_.erase(chunk_coord);
                storage_.delete_chunk(chunk_coord);
            } else {
                storage_.save_chunk(chunk_coord, chunk);
            }
        }
    }
    
    std::optional<VoxelData> get_voxel(const VoxelCoord& coord) {
        ChunkCoord chunk_coord = ChunkCoord::from_voxel(coord);
        VoxelChunk& chunk = load_or_create_chunk(chunk_coord);
        return chunk.get_voxel(coord);
    }
    
    // Bulk operations
    size_t fill_region(const BoundingBox& bbox, VoxelData value) {
        size_t total_changes = 0;
        ++current_version_;
        
        for (const auto& chunk_coord : bbox.intersecting_chunks()) {
            VoxelChunk& chunk = load_or_create_chunk(chunk_coord);
            
            // Convert world bounding box to chunk-local coordinates
            VoxelCoord chunk_base{chunk_coord.x * 32, chunk_coord.y * 32, chunk_coord.z * 32};
            VoxelCoord local_start = bbox.min - chunk_base;
            VoxelCoord local_end = bbox.max - chunk_base;
            
            size_t changes = chunk.fill_region(local_start, local_end, value, current_version_, node_id_);
            if (changes > 0) {
                total_changes += changes;
                storage_.save_chunk(chunk_coord, chunk);
            }
        }
        
        return total_changes;
    }
    
    // Spatial queries
    std::vector<std::pair<VoxelCoord, VoxelData>> get_region(const BoundingBox& bbox) {
        std::vector<std::pair<VoxelCoord, VoxelData>> result;
        
        for (const auto& chunk_coord : bbox.intersecting_chunks()) {
            VoxelChunk& chunk = load_or_create_chunk(chunk_coord);
            
            VoxelCoord chunk_base{chunk_coord.x * 32, chunk_coord.y * 32, chunk_coord.z * 32};
            VoxelCoord local_start = bbox.min - chunk_base;
            VoxelCoord local_end = bbox.max - chunk_base;
            
            auto chunk_voxels = chunk.get_region(local_start, local_end);
            for (auto& [local_coord, value] : chunk_voxels) {
                result.emplace_back(local_coord + chunk_base, value);
            }
        }
        
        return result;
    }
    
    // Get 6-connected neighbors (for physics/collision)
    std::array<std::optional<VoxelData>, 6> get_neighbors(const VoxelCoord& coord) {
        static const std::array<VoxelCoord, 6> offsets = {{
            {-1, 0, 0}, {1, 0, 0},  // left, right
            {0, -1, 0}, {0, 1, 0},  // down, up  
            {0, 0, -1}, {0, 0, 1}   // back, front
        }};
        
        std::array<std::optional<VoxelData>, 6> neighbors;
        for (size_t i = 0; i < 6; ++i) {
            neighbors[i] = get_voxel(coord + offsets[i]);
        }
        return neighbors;
    }
    
    void merge_changes(const std::vector<VoxelChange>& changes) {
        std::unordered_set<ChunkCoord, ChunkCoordHash> dirty_chunks;
        
        for (const auto& change : changes) {
            ChunkCoord chunk_coord = ChunkCoord::from_voxel(change.coord);
            VoxelChunk& chunk = load_or_create_chunk(chunk_coord);
            
            if (chunk.set_voxel(change.coord, change.value, change.version, change.node_id)) {
                current_version_ = std::max(current_version_, change.version);
                dirty_chunks.insert(chunk_coord);
            }
        }
        
        // Persist all modified chunks
        for (const auto& coord : dirty_chunks) {
            auto it = chunks_.find(coord);
            if (it != chunks_.end()) {
                if (it->second.is_empty()) {
                    chunks_.erase(it);
                    storage_.delete_chunk(coord);
                } else {
                    storage_.save_chunk(coord, it->second);
                }
            }
        }
    }
    
    std::vector<VoxelChange> get_changes_since(uint64_t since_version) {
        std::vector<VoxelChange> all_changes;
        
        // Check in-memory chunks first
        for (const auto& [chunk_coord, chunk] : chunks_) {
            if (chunk.get_chunk_version() > since_version) {
                auto chunk_changes = chunk.get_changes_since(chunk_coord, since_version);
                all_changes.insert(all_changes.end(), chunk_changes.begin(), chunk_changes.end());
            }
        }
        
        // TODO: Also check storage for chunks not currently loaded
        // This would require storing version metadata separately
        
        return all_changes;
    }
    
    // Force compression on all loaded chunks
    void compress_all() {
        for (auto& [coord, chunk] : chunks_) {
            chunk.force_compress();
            storage_.save_chunk(coord, chunk);
        }
    }
    
    uint64_t get_version() const { return current_version_; }
    size_t chunk_count() const { return chunks_.size(); }
    size_t total_voxel_count() const {
        size_t total = 0;
        for (const auto& [_, chunk] : chunks_) {
            total += chunk.active_voxel_count();
        }
        return total;
    }
    
    StorageBackend& get_storage() { return storage_; }
};

// Implementation of VoxelChunk serialization
std::vector<uint8_t> VoxelChunk::serialize() const {
    const_cast<VoxelChunk*>(this)->compress(); // Compress before serializing
    
    std::vector<uint8_t> result;
    result.reserve(1024); // Reasonable initial size
    
    // Header: version + flags + compressed size
    uint64_t header = chunk_version_;
    result.insert(result.end(), reinterpret_cast<const uint8_t*>(&header), 
                  reinterpret_cast<const uint8_t*>(&header) + sizeof(header));
    
    uint32_t segment_count = static_cast<uint32_t>(rle_data_.size());
    result.insert(result.end(), reinterpret_cast<const uint8_t*>(&segment_count),
                  reinterpret_cast<const uint8_t*>(&segment_count) + sizeof(segment_count));
    
    // RLE data
    for (const auto& segment : rle_data_) {
        result.insert(result.end(), reinterpret_cast<const uint8_t*>(&segment.value),
                      reinterpret_cast<const uint8_t*>(&segment.value) + sizeof(segment.value));
        result.insert(result.end(), reinterpret_cast<const uint8_t*>(&segment.run_length),
                      reinterpret_cast<const uint8_t*>(&segment.run_length) + sizeof(segment.run_length));
    }
    
    return result;
}

std::optional<VoxelChunk> VoxelChunk::deserialize(const std::vector<uint8_t>& data) {
    if (data.size() < sizeof(uint64_t) + sizeof(uint32_t)) {
        return std::nullopt;
    }
    
    VoxelChunk chunk;
    size_t offset = 0;
    
    // Read header
    std::memcpy(&chunk.chunk_version_, data.data() + offset, sizeof(uint64_t));
    offset += sizeof(uint64_t);
    
    uint32_t segment_count;
    std::memcpy(&segment_count, data.data() + offset, sizeof(uint32_t));
    offset += sizeof(uint32_t);
    
    // Read RLE segments
    chunk.rle_data_.reserve(segment_count);
    for (uint32_t i = 0; i < segment_count; ++i) {
        if (offset + sizeof(VoxelData) + sizeof(uint16_t) > data.size()) {
            return std::nullopt;
        }
        
        VoxelData value;
        uint16_t run_length;
        
        std::memcpy(&value, data.data() + offset, sizeof(VoxelData));
        offset += sizeof(VoxelData);
        
        std::memcpy(&run_length, data.data() + offset, sizeof(uint16_t));
        offset += sizeof(uint16_t);
        
        chunk.rle_data_.emplace_back(value, run_length);
    }
    
    chunk.compressed_ = true;
    return chunk;
}

} // namespace voxel_crdt