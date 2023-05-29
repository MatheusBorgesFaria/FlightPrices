import os
import sys
from glob import glob

import paramiko

sys.path.append("../utils")
import tools
from get_ip import get_public_ip


def file_transfer(local_file, destination_path,
                  host, user, password,
                  deleting_local_file=False):
    """
    Transfer a file from the source path to the destination path on a remote server using SSH and SFTP.

    Parameters
    ----------
        local_file: str
            The path of the file to transfer.
        destination_path: str
            The destination path on the remote server.
        host: str
            The hostname or IP address of the remote server.
        user: str
            The user for authentication.
        password: str
            The password for authentication.

    Raises
    ------
        paramiko.AuthenticationException: If there is an authentication error.
        paramiko.SSHException: If there is an SSH-related error.
        Exception: For other exceptions that may occur.
    """
    # Create an instance of the SSH client
    ssh_client = paramiko.SSHClient()
    ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

    try:
        # Connect to the remote server
        ssh_client.connect(host, username=user, password=password)

        # Create an SFTP client
        sftp_client = ssh_client.open_sftp()

        # Transfer the file from the source directory to the destination directory
        sftp_client.put(local_file, destination_path)
        print("File transferred successfully!")
        
        if deleting_local_file:
            print("Deleting local file!")
            os.remove(local_file)
        
    except paramiko.AuthenticationException:
        print("Authentication error. Please check the login credentials.")
    except paramiko.SSHException as e:
        print(f"SSH error: {str(e)}")
    except Exception as e:
        print(f"Error: {str(e)}")
    finally:
        # Close the SSH connection
        ssh_client.close()
    return

        
def structured_data_transfer(paths_to_transfer=None, deleting_local_file=True):
    """Transfer structured data files to destination folder with optional deletion of local files.

    Parameters
    ----------
    paths_to_transfer: list[str] or srt
        The paths files list to transfer.
        
    deleting_local_file: bool (default=True)
        Flag indicating whether to delete local files after transfer.
    """
    structured_data_origin = tools.get_relevant_path("structured_data")
    
    if paths_to_transfer is None:
        paths_to_transfer = glob(
            os.path.join(structured_data_origin , "*.parquet")
        )
    elif isinstance(paths_to_transfer, str):
        paths_to_transfer = [paths_to_transfer]
    
    assert isinstance(paths_to_transfer, list), "paths_to_transfer must be list[str] or str."

    ip = get_public_ip()
    structured_data_destination = tools.get_relevant_path("structured_data_destination")
    structured_data_transfer_credentials = tools.get_structured_data_transfer_credentials()    
    destination_ip = structured_data_transfer_credentials.get("host", "")

    if ip != destination_ip:
        for local_file_path in paths_to_transfer:
            file_name = os.path.basename(local_file_path)

            print(f"Moving the file {file_name}...")
            destination_path = os.path.join(
                structured_data_destination, (f"ip={ip}_" + file_name)
            )
            file_transfer(local_file=local_file_path,
                          destination_path=destination_path,
                          **structured_data_transfer_credentials,
                          deleting_local_file=deleting_local_file)
    return