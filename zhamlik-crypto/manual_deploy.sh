#!/bin/bash
# Manual deployment script for Zhamlik Crypto
# Run this script and enter the SSH password when prompted

echo "=== Zhamlik Crypto Deployment ==="
echo "You will be prompted for SSH password (user: root@193.29.224.20)"
echo ""

# Function to execute command on remote server
remote_exec() {
    ssh -o StrictHostKeyChecking=no root@193.29.224.20 "$1"
}

# Function to copy file to remote server
remote_copy() {
    scp -o StrictHostKeyChecking=no "$1" root@193.29.224.20:"$2"
}

echo "Step 1: Copying app.py..."
remote_copy "/home/onor/projects/zhamlik-crypto/app.py" "/opt/zhamlik-crypto/app.py"

echo "Step 2: Copying static files..."
ssh -o StrictHostKeyChecking=no root@193.29.224.20 "rm -rf /opt/zhamlik-crypto/static/*"
scp -r -o StrictHostKeyChecking=no /home/onor/projects/zhamlik-crypto/static/* root@193.29.224.20:/opt/zhamlik-crypto/static/

echo "Step 3: Copying template files..."
ssh -o StrictHostKeyChecking=no root@193.29.224.20 "rm -rf /opt/zhamlik-crypto/templates/*"
scp -r -o StrictHostKeyChecking=no /home/onor/projects/zhamlik-crypto/templates/* root@193.29.224.20:/opt/zhamlik-crypto/templates/

echo "Step 4: Restarting service..."
remote_exec "systemctl restart zhamlik-crypto"

echo ""
echo "=== Deployment Complete! ==="
echo "App is available at: http://193.29.224.20:5004"
