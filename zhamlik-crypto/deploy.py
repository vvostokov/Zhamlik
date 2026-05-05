#!/usr/bin/env python3
"""Deployment script for Zhamlik Crypto"""

import os
import paramiko
import scp
import sys

SERVER = '193.29.224.20'
USER = 'root'
PASSWORD = 'omer4472'
REMOTE_PATH = '/opt/zhamlik-crypto/'
LOCAL_PATH = '/home/onor/projects/zhamlik-crypto/'

def deploy():
    print("Connecting to server...")
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())

    try:
        ssh.connect(SERVER, username=USER, password=PASSWORD)
        print("Connected successfully")

        # Copy files
        print("Copying files...")
        with scp.SCPClient(ssh.get_transport()) as s:
            # Copy app.py
            s.put(os.path.join(LOCAL_PATH, 'app.py'), REMOTE_PATH + 'app.py')

            # Copy requirements.txt
            s.put(os.path.join(LOCAL_PATH, 'requirements.txt'), REMOTE_PATH + 'requirements.txt')

            # Copy static directory
            for root, dirs, files in os.walk(os.path.join(LOCAL_PATH, 'static')):
                for file in files:
                    local_file = os.path.join(root, file)
                    remote_file = os.path.join(REMOTE_PATH, os.path.relpath(local_file, LOCAL_PATH))
                    # Create remote directory if needed
                    remote_dir = os.path.dirname(remote_file)
                    try:
                        ssh.exec_command(f'mkdir -p {remote_dir}')
                    except:
                        pass
                    s.put(local_file, remote_file)

            # Copy templates directory
            for root, dirs, files in os.walk(os.path.join(LOCAL_PATH, 'templates')):
                for file in files:
                    local_file = os.path.join(root, file)
                    remote_file = os.path.join(REMOTE_PATH, os.path.relpath(local_file, LOCAL_PATH))
                    # Create remote directory if needed
                    remote_dir = os.path.dirname(remote_file)
                    try:
                        ssh.exec_command(f'mkdir -p {remote_dir}')
                    except:
                        pass
                    s.put(local_file, remote_file)

        print("Files copied successfully")

        # Restart service
        print("Restarting service...")
        stdin, stdout, stderr = ssh.exec_command('systemctl restart zhamlik-crypto')
        exit_status = stdout.channel.recv_exit_status()

        if exit_status == 0:
            print("Service restarted successfully")
        else:
            print(f"Warning: Service restart failed with code {exit_status}")
            print(stderr.read().decode())

        print("\nDeployment complete!")
        print("App is available at: http://193.29.224.20:5004")

    except Exception as e:
        print(f"Deployment failed: {e}", file=sys.stderr)
        sys.exit(1)
    finally:
        ssh.close()

if __name__ == '__main__':
    # Check if paramiko is installed
    try:
        import paramiko
        import scp
    except ImportError:
        print("Installing required packages...")
        os.system('pip3 install paramiko scp --user')
        print("Please run the script again")
        sys.exit(1)

    deploy()
