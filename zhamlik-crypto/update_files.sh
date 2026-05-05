#!/bin/bash
# Script to run on the server
# Usage: ssh root@193.29.224.20 'bash -s' < update_files.sh

set -e

echo "Updating Zhamlik Crypto application..."

# Create temp directory
TEMP_DIR=$(mktemp -d)
cd "$TEMP_DIR"

# Download files from local project
echo "Downloading files..."

# Copy files using cat and heredoc
cat > app.py << 'APPEOF'
$(cat /home/onor/projects/zhamlik-crypto/app.py)
APPEOF

echo "Files updated. Copying to /opt/zhamlik-crypto..."

# Copy to deployment directory
cp app.py /opt/zhamlik-crypto/
cp -r /home/onor/projects/zhamlik-crypto/static/* /opt/zhamlik-crypto/static/
cp -r /home/onor/projects/zhamlik-crypto/templates/* /opt/zhamlik-crypto/templates/

# Restart service
systemctl restart zhamlik-crypto

echo "Deployment complete!"
cd -
rm -rf "$TEMP_DIR"
