helm version --short 2>/dev/null; kubectl version --client --short 2>/dev/null || kubectl version --client

command -v helm || true; brew list helm >/dev/null 2>&1 && echo helm-brew || t

command -v brew; command -v helm; ls /opt/homebrew/bin/helm /usr/local/bin/helm 2>/dev/null || true

brew install helm

helm repo add kubernetes-dashboard https://kubernetes-retired.github.io/dashboard/ && helm repo update kubernetes-dashboard && helm upgrade --install kubernetes-dashboard kubernetes-dashboard/kubernetes-dashboard --create-namespace --namespace kubernetes-dashboard


kubectl apply -f - <<'EOF'
apiVersion: v1
kind: ServiceAccount
metadata:
  name: admin-user
  namespace: kubernetes-dashboard
---
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRoleBinding
metadata:
  name: admin-user
roleRef:
  apiGroup: rbac.authorization.k8s.io
  kind: ClusterRole
  name: cluster-admin
subjects:
- kind: ServiceAccount
  name: admin-user
  namespace: kubernetes-dashboard
---
apiVersion: v1
kind: Secret
metadata:
  name: admin-user
  namespace: kubernetes-dashboard
  annotations:
    kubernetes.io/service-account.name: admin-user
type: kubernetes.io/service-account-token
EOF

kubectl -n kubernetes-dashboard rollout status deploy --timeout=180s

kubectl -n kubernetes-dashboard get pods,svc

kubectl -n kubernetes-dashboard port-forward --address 127.0.0.1 svc/kubernetes-dashboard-kong-proxy 8443:443
# https://localhost:8443/#/workloads?namespace=pricestream

kubectl -n kubernetes-dashboard create token admin-user

