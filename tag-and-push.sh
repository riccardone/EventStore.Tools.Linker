#!/bin/bash

# Prompt for version
read -p "Enter the release version (e.g., 1.0.0): " version

# Ensure it's not empty
if [[ -z "$version" ]]; then
  echo "Version cannot be empty. Aborting."
  exit 1
fi

# Format tag with 'v' prefix
tag="v$version"

# Check if tag already exists
if git rev-parse "$tag" >/dev/null 2>&1; then
  echo "Tag '$tag' already exists locally. Aborting."
  exit 1
fi

# Show summary and ask for confirmation
echo "About to create and push Git tag: $tag"
read -p "Proceed? (y/n): " confirm
if [[ "$confirm" != "y" && "$confirm" != "Y" ]]; then
  echo "Aborted by user."
  exit 0
fi

# Create and push the tag
git tag "$tag"
git push origin "$tag"

echo "✅ Tag '$tag' created and pushed successfully."
