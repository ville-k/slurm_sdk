#!/bin/bash
set -e

ENROOT_VERSION="3.4.1"

echo "Installing enroot ${ENROOT_VERSION}..."

# Detect architecture
ARCH=$(dpkg --print-architecture)
echo "Detected architecture: ${ARCH}"

# Install dependencies
apt-get update
apt-get install -y \
    squashfs-tools \
    fuse-overlayfs \
    parallel \
    curl \
    jq \
    pigz \
    zstd \
    bsdmainutils \
    libcap2-bin

# Download and install enroot
cd /tmp
echo "Downloading enroot packages for ${ARCH}..."
curl -fSsL -O "https://github.com/NVIDIA/enroot/releases/download/v${ENROOT_VERSION}/enroot_${ENROOT_VERSION}-1_${ARCH}.deb"
curl -fSsL -O "https://github.com/NVIDIA/enroot/releases/download/v${ENROOT_VERSION}/enroot+caps_${ENROOT_VERSION}-1_${ARCH}.deb"

echo "Installing enroot packages..."
dpkg -i enroot_${ENROOT_VERSION}-1_${ARCH}.deb
dpkg -i enroot+caps_${ENROOT_VERSION}-1_${ARCH}.deb

# Configure enroot
echo "Configuring enroot..."
mkdir -p /etc/enroot
cat > /etc/enroot/enroot.conf << 'EOF'
# enroot configuration for slurm-pyxis-integration testing

# Runtime paths
ENROOT_RUNTIME_PATH /run/enroot/user-$(id -u)
ENROOT_CACHE_PATH /var/lib/enroot/cache/user-$(id -u)
ENROOT_DATA_PATH /var/lib/enroot/data/user-$(id -u)
ENROOT_TEMP_PATH /tmp

# Allow unprivileged user namespaces
ENROOT_REMAP_ROOT yes

# Image import settings
ENROOT_MAX_PROCESSORS $(nproc)

# Allow HTTP registries (for testing with local registry)
ENROOT_ALLOW_HTTP yes
EOF

# Create enroot directories with proper permissions
mkdir -p /var/lib/enroot/cache
mkdir -p /var/lib/enroot/data
mkdir -p /run/enroot
chmod 1777 /var/lib/enroot/cache
chmod 1777 /var/lib/enroot/data
chmod 1777 /run/enroot

# Cleanup
echo "Cleaning up..."
rm -f /tmp/enroot_${ENROOT_VERSION}-1_${ARCH}.deb
rm -f /tmp/enroot+caps_${ENROOT_VERSION}-1_${ARCH}.deb

echo "enroot installation complete!"
