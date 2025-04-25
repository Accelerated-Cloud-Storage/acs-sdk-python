# --- Basic File Symlink Tests ---
# Create test content
echo "test content" > test_file
ls -la test_file  # Verify file creation

# Create and verify basic symlink
ln -s test_file test_link
ls -la test_*     # Should show both file and symlink
readlink test_link  # Should show target path
cat test_link    # Should show content through symlink

# --- Directory Symlink Tests ---
# Create directory structure
mkdir -p test_dir/subdir
echo "file1 content" > test_dir/file1
echo "file2 content" > test_dir/subdir/file2

# Create directory symlink
ln -s test_dir dir_link
ls -la dir_link  # Should show directory contents
cat dir_link/file1  # Read through symlink
cat dir_link/subdir/file2  # Test nested access

# --- Path Resolution Tests ---
# Test relative path symlinks
mkdir -p dir1/dir2
echo "nested file" > dir1/dir2/file
cd dir1
ln -s dir2/file rel_link
cat rel_link  # Should work
cd ..

# Test absolute path symlinks
ln -s "$(pwd)/test_file" abs_link
cat abs_link  # Should work

# --- Edge Cases ---
# Broken symlinks
ln -s nonexistent broken_link
ls -la broken_link  # Should show as broken
readlink broken_link  # Should still work

# Symlink chains
ln -s test_file link1
ln -s link1 link2
ln -s link2 link3
cat link3  # Should follow chain

# --- File Operations Through Symlinks ---
# Write operations
echo "new content" > test_link  # Write through symlink
cat test_file  # Verify original updated
cat test_link  # Verify symlink sees update

# Directory operations through symlinks
mkdir -p dir_link/new_subdir  # Create dir through symlink
echo "new file" > dir_link/new_subdir/file3
ls -la test_dir/new_subdir  # Verify in original
cat test_dir/new_subdir/file3  # Verify content

# --- Attribute Tests ---
# Check symlink attributes
stat test_link  # Check symlink stats
stat -L test_link  # Check target stats through symlink

# Extended attributes (if supported)
getfattr -d test_link
getfattr -d -m - -e hex test_link  # Raw attribute view

# --- Cleanup ---
# Remove symlinks first
rm test_link dir_link broken_link link1 link2 link3 abs_link
cd dir1 && rm rel_link && cd ..

# Remove test content
rm -rf test_file test_dir dir1

# --- Special Cases ---
# Test symlink to root of mount
ln -s / root_link
ls -la root_link
rm root_link

# Test symlink with special characters
echo "special" > "file with spaces.txt"
ln -s "file with spaces.txt" special_link
cat special_link
rm "file with spaces.txt" special_link

# Test concurrent access
for i in {1..5}; do cat test_link & done  # Parallel reads
wait  # Wait for all background processes