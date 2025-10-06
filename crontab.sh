#!/bin/bash

# This script replaces the crontab with the jobs defined below
# Run python sf_data_pipelines covariance_matrix every day at 2am MT

# Create a temporary file with the new crontab content
TEMP_CRON=$(mktemp)

# Write the crontab entries to the temporary file
cat > "$TEMP_CRON" << 'EOF'
# Run covariance_matrix at 2am MT (Mountain Time)
# Note: Cron uses system time, so adjust if your system isn't set to MT
0 2 * * * /home/amh1124/Projects/sf-data-pipelines/.venv/bin/python /home/amh1124/Projects/sf-data-pipelines/sf_data_pipelines covariance-matrix
EOF

# Install the new crontab
crontab "$TEMP_CRON"

# Clean up
rm "$TEMP_CRON"

echo "Crontab updated successfully!"
echo "Current crontab:"
crontab -l