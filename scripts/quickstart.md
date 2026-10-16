Tab 1:
skaffold run

Tab 2:
kubectl port-forward -n pricestream svc/grafana 3000:3000

Tab 3:
kubectl port-forward -n pricestream svc/pricestream 8081:80

Tab 4:
VITE_PROXY_TARGET=http://localhost:8080 npm run dev