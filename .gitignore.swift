# Swift-specific .gitignore additions
# Merge this with your existing .gitignore

# Xcode
*.xcodeproj
*.xcworkspace
!default.xcworkspace
xcuserdata/
*.moved-aside
*.xccheckout
*.xcscmblueprint

# Swift Package Manager
/.build
/Packages
Package.resolved
*.swiftpm
.swiftpm/

# CocoaPods
Pods/
Podfile.lock

# Carthage
Carthage/Build

# SPM
.swiftpm/xcode/package.xcworkspace/contents.xcworkspacedata

# macOS
.DS_Store
.AppleDouble
.LSOverride

# Xcode Patch
*.xcodeproj/*
!*.xcodeproj/project.pbxproj
!*.xcodeproj/xcshareddata/
!*.xcworkspace/contents.xcworkspacedata

# Swift generated files
*.generated.swift

# Playgrounds
timeline.xctimeline
playground.xcworkspace

# Build artifacts
DerivedData/
*.dSYM.zip
*.dSYM
