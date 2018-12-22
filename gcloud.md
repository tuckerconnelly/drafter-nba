```
gcloud beta compute --project=vaulted-timing-226306 instances create drafter \
  --zone=us-west1-a \
  --machine-type=n1-highmem-4 \
  --subnet=default \
  --network-tier=PREMIUM \
  --no-restart-on-failure \
  --maintenance-policy=TERMINATE \
  --preemptible \
  --service-account=337712110664-compute@developer.gserviceaccount.com \
  --scopes=https://www.googleapis.com/auth/devstorage.read_only,https://www.googleapis.com/auth/logging.write,https://www.googleapis.com/auth/monitoring.write,https://www.googleapis.com/auth/servicecontrol,https://www.googleapis.com/auth/service.management.readonly,https://www.googleapis.com/auth/trace.append \
  --image=ubuntu-minimal-1804-bionic-v20181220 \
  --image-project=ubuntu-os-cloud \
  --boot-disk-size=10GB \
  --boot-disk-type=pd-ssd \
  --boot-disk-device-name=drafter

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
export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/usr/local/cuda/extras/CUPTI/lib64
sudo apt-key adv --fetch-keys http://developer.download.nvidia.com/compute/cuda/repos/ubuntu1604/x86_64/7fa2af80.pub
wget http://developer.download.nvidia.com/compute/cuda/repos/ubuntu1604/x86_64/cuda-repo-ubuntu1604_9.1.85-1_amd64.debew
sudo apt install ./cuda-repo-ubuntu1604_9.1.85-1_amd64.deb
wget http://developer.download.nvidia.com/compute/machine-learning/repos/ubuntu1604/x86_64/nvidia-machine-learning-repo-ubuntu1604_1.0.0-1_amd64.deb
sudo apt install ./nvidia-machine-learning-repo-ubuntu1604_1.0.0-1_amd64.deb
sudo apt update
sudo apt install cuda9.0 cuda-cublas-9-0 cuda-cufft-9-0 cuda-curand-9-0 \
    cuda-cusolver-9-0 cuda-cusparse-9-0 libcudnn7=7.2.1.38-1+cuda9.0 \
    libnccl2=2.2.13-1+cuda9.0 cuda-command-line-tools-9-0
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
