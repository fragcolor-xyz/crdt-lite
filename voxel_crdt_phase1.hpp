#pragma once

#include <cstdint>
#include <unordered_map>
#include <vector>
#include <optional>
#include <array>
#include <bitset>
#include <algorithm>
#include <functional>

namespace voxel_crdt {

// Phase 1: Essential voxel CRDT with chunked storage
// Focus: memory efficiency, basic CRDT operations, spatial locality

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

class VoxelChunk {
    static constexpr size_t CHUNK_VOLUME = ChunkSize::value * ChunkSize::value * ChunkSize::value;
    
    std::array<VoxelData, CHUNK_VOLUME> data_{};
    std::array<VoxelVersion, CHUNK_VOLUME> versions_{};
    std::bitset<CHUNK_VOLUME> active_{}; // track which voxels are set
    
    uint64_t chunk_version_ = 0;
    
    static constexpr size_t coord_to_index(int x, int y, int z) {
        return (z * ChunkSize::value + y) * ChunkSize::value + x;
    }
    
    static constexpr size_t local_coord_to_index(const VoxelCoord& local) {
        return coord_to_index(local.x & 31, local.y & 31, local.z & 31);
    }

public:
    std::optional<VoxelData> get_voxel(const VoxelCoord& local_coord) const {
        size_t idx = local_coord_to_index(local_coord);
        return active_[idx] ? std::optional{data_[idx]} : std::nullopt;
    }
    
    bool set_voxel(const VoxelCoord& local_coord, std::optional<VoxelData> value, 
                   uint64_t version, NodeId node_id) {
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
    
    uint64_t get_chunk_version() const { return chunk_version_; }
    
    std::vector<VoxelChange> get_changes_since(const ChunkCoord& chunk_coord, uint64_t since_version) const {
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
};

class VoxelCRDT {
    std::unordered_map<ChunkCoord, VoxelChunk, ChunkCoordHash> chunks_;
    NodeId node_id_;
    uint64_t current_version_ = 0;
    
public:
    explicit VoxelCRDT(NodeId node_id) : node_id_(node_id) {}
    
    void set_voxel(const VoxelCoord& coord, VoxelData value) {
        ChunkCoord chunk_coord = ChunkCoord::from_voxel(coord);
        VoxelChunk& chunk = chunks_[chunk_coord];
        
        ++current_version_;
        chunk.set_voxel(coord, value, current_version_, node_id_);
    }
    
    void remove_voxel(const VoxelCoord& coord) {
        ChunkCoord chunk_coord = ChunkCoord::from_voxel(coord);
        auto it = chunks_.find(chunk_coord);
        if (it != chunks_.end()) {
            ++current_version_;
            it->second.set_voxel(coord, std::nullopt, current_version_, node_id_);
            
            // Remove empty chunks to save memory
            if (it->second.is_empty()) {
                chunks_.erase(it);
            }
        }
    }
    
    std::optional<VoxelData> get_voxel(const VoxelCoord& coord) const {
        ChunkCoord chunk_coord = ChunkCoord::from_voxel(coord);
        auto it = chunks_.find(chunk_coord);
        return it != chunks_.end() ? it->second.get_voxel(coord) : std::nullopt;
    }
    
    void merge_changes(const std::vector<VoxelChange>& changes) {
        for (const auto& change : changes) {
            ChunkCoord chunk_coord = ChunkCoord::from_voxel(change.coord);
            VoxelChunk& chunk = chunks_[chunk_coord];
            
            if (chunk.set_voxel(change.coord, change.value, change.version, change.node_id)) {
                current_version_ = std::max(current_version_, change.version);
            }
            
            // Clean up empty chunks
            if (chunk.is_empty()) {
                chunks_.erase(chunk_coord);
            }
        }
    }
    
    std::vector<VoxelChange> get_changes_since(uint64_t since_version) const {
        std::vector<VoxelChange> all_changes;
        
        for (const auto& [chunk_coord, chunk] : chunks_) {
            if (chunk.get_chunk_version() > since_version) {
                auto chunk_changes = chunk.get_changes_since(chunk_coord, since_version);
                all_changes.insert(all_changes.end(), chunk_changes.begin(), chunk_changes.end());
            }
        }
        
        return all_changes;
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
};

} // namespace voxel_crdt