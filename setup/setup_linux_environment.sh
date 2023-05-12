# Setup built on a Fedora Linux distribution

# Install useful packages
sudo dnf install tmux python cronie postgresql-server
pip install jupyterlab

# Start cron service
sudo systemctl start crond.service

# Enable the cron service so that it starts automatically during system startup
sudo systemctl enable crond.service

# Set new tmux config
chmod +x change_tmux_config.sh
./change_tmux_config.sh

# Start the PostgreSQL service
sudo /usr/bin/postgresql-setup --initdb
sudo systemctl start postgresql

# Configure PostgreSQL to start automatically with the system
sudo systemctl enable postgresql

# Create a database named flight
sudo -i -u postgres psql < flight_database_config.sql
