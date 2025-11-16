#!/bin/bash
set -e

PYXIS_VERSION="0.19.0"

echo "Installing Pyxis ${PYXIS_VERSION}..."

# Install build dependencies
apt-get update
apt-get install -y \
    git \
    gcc \
    make \
    libslurm-dev

# Clone and build Pyxis
echo "Cloning Pyxis repository..."
cd /tmp
git clone --depth 1 --branch "v${PYXIS_VERSION}" https://github.com/NVIDIA/pyxis.git
cd pyxis

echo "Building Pyxis..."
make

echo "Installing Pyxis..."
make install

# Configure Pyxis SPANK plugin
echo "Configuring Pyxis SPANK plugin..."
mkdir -p /etc/slurm

# Create plugstack.conf to load Pyxis plugin
cat > /etc/slurm/plugstack.conf << 'EOF'
# SPANK plugin configuration for Pyxis
# See: https://github.com/NVIDIA/pyxis

# Load Pyxis plugin configuration
include /usr/local/share/pyxis/pyxis.conf
EOF

# Verify installation
if [ -f /usr/local/lib/slurm/spank_pyxis.so ]; then
    echo "Pyxis SPANK plugin installed at: /usr/local/lib/slurm/spank_pyxis.so"
else
    echo "Warning: Pyxis SPANK plugin not found at expected location"
fi

# Cleanup
echo "Cleaning up..."
cd /
rm -rf /tmp/pyxis

echo "Pyxis installation complete!"
