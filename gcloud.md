```
# Create ubuntu 16.04 image w/ p100

gcloud compute --project "vaulted-timing-226306" ssh --zone "us-west1-a" "drafter"
```

```
# Node
sudo apt-get update
sudo apt-get install git
curl -sL https://deb.nodesource.com/setup_11.x | sudo -E bash -
sudo apt-get install -y nodejs
sudo apt-get install gcc g++ make
sudo apt-get install postgresql postgresql-contrib
sudo apt-get install libpq-dev


# GPU
echo "Checking for CUDA and installing."

# Check for CUDA and try to install.
if ! dpkg-query -W cuda-9-0; then
  # The 16.04 installer works with 16.10.
  curl -O http://developer.download.nvidia.com/compute/cuda/repos/ubuntu1604/x86_64/cuda-repo-ubuntu1604_9.0.176-1_amd64.deb
  dpkg -i ./cuda-repo-ubuntu1604_9.0.176-1_amd64.deb
  apt-key adv --fetch-keys http://developer.download.nvidia.com/compute/cuda/repos/ubuntu1604/x86_64/7fa2af80.pub
  apt-get update
  apt-get install cuda-9-0 -y
fi

# Enable persistence mode
nvidia-smi -pm 1

# Add NVIDIA package repository
sudo apt-key adv --fetch-keys http://developer.download.nvidia.com/compute/cuda/repos/ubuntu1604/x86_64/7fa2af80.pub
wget http://developer.download.nvidia.com/compute/cuda/repos/ubuntu1604/x86_64/cuda-repo-ubuntu1604_9.1.85-1_amd64.deb
sudo apt install ./cuda-repo-ubuntu1604_9.1.85-1_amd64.deb
wget http://developer.download.nvidia.com/compute/machine-learning/repos/ubuntu1604/x86_64/nvidia-machine-learning-repo-ubuntu1604_1.0.0-1_amd64.deb
sudo apt install ./nvidia-machine-learning-repo-ubuntu1604_1.0.0-1_amd64.deb
sudo apt update

# Install CUDA and tools. Include optional NCCL 2.x
sudo apt install cuda9.0 cuda-cublas-9-0 cuda-cufft-9-0 cuda-curand-9-0 \
    cuda-cusolver-9-0 cuda-cusparse-9-0 libcudnn7=7.2.1.38-1+cuda9.0 \
    libnccl2=2.2.13-1+cuda9.0 cuda-command-line-tools-9-0

# Optional: Install the TensorRT runtime (must be after CUDA install)
sudo apt update
sudo apt install libnvinfer4=4.1.2-1+cuda9.0


git clone https://@github.com/tuckerconnelly/drafter-nba.git
cd drafter-nba
npm i
cp .env.examle .env
echo "Update .env values"
```

```
brew cask install osxfuse
brew install sshfs
sshfs [instance-ip]:/home/tuckerconnelly ~/sshfs/drafter
```
