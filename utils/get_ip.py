import socket


def get_public_ip():
    """Retrieve the public IP address of the current machine.

    This function creates an IPv4 socket, connects to a DNS server
    (in this case, Google's public DNS server), retrieves the IP address
    associated with the socket, and closes the socket.

    Return
    ------
        ip_address: str
            The public IP address of the machine, or None if the IP address
            couldn't be obtained.
    """
    try:
        # Create an IPv4 socket
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

        # Connect to a DNS server
        sock.connect(('8.8.8.8', 80))

        # Get the IP address from the socket
        ip_address = sock.getsockname()[0]

        # Close the socket
        sock.close()
         
    except socket.error:
        ip_address = None

    return ip_address