# Create directory for your application
cargo build --release
rm -rv /opt/divarel/
mkdir -p /opt/divarel

# Copy files
cp ./target/release/divarel /opt/divarel/ -fv

# Set proper permissions
chmod 755 /opt/divarel
