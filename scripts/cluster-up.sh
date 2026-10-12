#!/usr/bin/env bash
# Creates the kind cluster described in k8s/kind-cluster.yaml.
set -euo pipefail

cd "$(dirname "$0")/.."

if ! kind get clusters | grep -qx pricestream; then
  kind create cluster --config k8s/kind-cluster.yaml
fi

kubectl config use-context kind-pricestream
kubectl get nodes -L pricestream.io/pool
