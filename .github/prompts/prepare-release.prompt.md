---
description: "Prepare a NuGet package release — bump version, update changelog, create a git tag. Guides through the full release workflow."
---

# Prepare Release

I need to prepare a new release of the KafkaWorker NuGet packages.

## Steps
1. Review changes since the last release tag: `git log --oneline $(git describe --tags --abbrev=0)..HEAD`
2. Categorize changes: Breaking Changes, New Features, Bug Fixes, Internal
3. If there are breaking changes, bump the major version. New features bump minor. Bug fixes bump patch.
4. Update the README if any public API changes were made
5. Create and push a git tag: `git tag v{X.Y.Z} && git push origin v{X.Y.Z}`
6. The `publish.yml` workflow will automatically pack and push to NuGet on tag push

## Version Guidance
- Breaking change to `IMessageHandler`, `InvalidMessageException`, `KafkaWorkerConfig`, or `ServiceCollectionExtensions` → major bump
- New feature (e.g., new serialization format, new config option) → minor bump
- Bug fix or internal refactor → patch bump
