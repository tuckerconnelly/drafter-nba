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