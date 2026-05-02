#\!/bin/bash
# Quick update script for zhamlik
cd /opt/zhamlik
git fetch origin
git pull origin master
systemctl restart zhamlik
