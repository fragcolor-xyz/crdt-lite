// swift-tools-version: 5.7
// The swift-tools-version declares the minimum version of Swift required to build this package.

import PackageDescription

let package = Package(
    name: "CRDTLite",
    platforms: [
        .macOS(.v12),
        .iOS(.v15),
        .watchOS(.v8),
        .tvOS(.v15)
    ],
    products: [
        .library(
            name: "CRDTLite",
            targets: ["CRDTLite"]),
    ],
    targets: [
        .target(
            name: "CRDTLite",
            dependencies: []),
        .testTarget(
            name: "CRDTLiteTests",
            dependencies: ["CRDTLite"]),
    ]
)
