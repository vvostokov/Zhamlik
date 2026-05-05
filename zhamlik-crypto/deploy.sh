#!/bin/bash
# Deployment script for Zhamlik Crypto

set -e

echo "Deploying Zhamlik Crypto to server..."

# Copy files to server
echo "Copying files..."
scp -o StrictHostKeyChecking=no -r \
    app.py \
    requirements.txt \
    static/ \
    templates/ \
    root@193.29.224.20:/opt/zhamlik-crypto/

# Restart service
echo "Restarting service..."
ssh -o StrictHostKeyChecking=no root@193.29.224.20 "systemctl restart zhamlik-crypto"

echo "Deployment complete!"
echo "App is available at: http://193.29.224.20:5004"
