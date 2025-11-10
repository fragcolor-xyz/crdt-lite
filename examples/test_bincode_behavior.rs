//! Testing bincode's schema evolution behavior
//!
//! Bincode v2 is a compact binary format that may have limitations
//! for schema evolution compared to formats like JSON or MessagePack.

use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
struct VersionOld {
    a: u32,
    b: String,
}

#[derive(Debug, Serialize, Deserialize)]
struct VersionNewWithDefault {
    a: u32,
    b: String,
    #[serde(default)]
    c: u32,
}

#[derive(Debug, Serialize, Deserialize)]
struct VersionNewWithSkip {
    a: u32,
    b: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    c: Option<u32>,
}

fn main() {
    println!("=== Bincode Schema Evolution Testing ===\n");

    let old = VersionOld {
        a: 42,
        b: "hello".to_string(),
    };

    println!("Testing bincode v{}", env!("CARGO_PKG_VERSION"));
    println!("Bincode configuration: standard()");

    // Serialize old version
    let bytes = bincode::serde::encode_to_vec(&old, bincode::config::standard())
        .expect("Failed to serialize old");
    println!("\nOld struct serialized: {} bytes", bytes.len());
    println!("Hex: {:02x?}", &bytes[..]);

    // Try to deserialize with new struct (with default)
    println!("\n--- Attempting to deserialize with new field + #[serde(default)] ---");
    match bincode::serde::decode_from_slice::<VersionNewWithDefault, _>(
        &bytes,
        bincode::config::standard(),
    ) {
        Ok((v, _)) => println!("✓ Success: a={}, b={}, c={}", v.a, v.b, v.c),
        Err(e) => println!("✗ Failed: {}", e),
    }

    // Try with skip_serializing_if
    println!("\n--- Attempting with Option + skip_serializing_if ---");
    match bincode::serde::decode_from_slice::<VersionNewWithSkip, _>(
        &bytes,
        bincode::config::standard(),
    ) {
        Ok((v, _)) => println!("✓ Success: a={}, b={}, c={:?}", v.a, v.b, v.c),
        Err(e) => println!("✗ Failed: {}", e),
    }

    println!("\n=== Bincode Limitations ===");
    println!("Bincode is a length-prefixed binary format that:");
    println!("  • Encodes the exact number of fields");
    println!("  • Does NOT support adding/removing fields by default");
    println!("  • #[serde(default)] does NOT help (unlike JSON)");
    println!("  • Designed for exact struct matching, not evolution");

    println!("\n=== Alternative Approaches ===");
    println!("1. Use JSON/MessagePack instead of bincode");
    println!("   - Better schema evolution support");
    println!("   - Slightly larger file sizes");
    println!("   - More robust for long-term storage");

    println!("\n2. Implement explicit versioning:");
    println!("   - Wrap structs in versioned enums");
    println!("   - Detect version on load, migrate if needed");
    println!("   - Example:");
    println!("     enum VersionedRecord {{");
    println!("       V1(RecordV1),");
    println!("       V2(RecordV2),");
    println!("     }}");

    println!("\n3. Use a self-describing format:");
    println!("   - CBOR, MessagePack, or FlexBuffers");
    println!("   - Field names encoded in data");
    println!("   - Can skip unknown fields");
}
