#!/bin/bash
# Скрипт деплоя DAG в Argo Workflows

set -e

NAMESPACE="argo"

echo "=== Deploying AstanaHub Reports DAG ==="

# 1. Создаём ConfigMap со скриптами
echo "Creating ConfigMap with Spark scripts..."
kubectl create configmap spark-scripts \
    --from-file=process_reports.py=spark-jobs/process_reports.py \
    --namespace=$NAMESPACE \
    --dry-run=client -o yaml | kubectl apply -f -

# 2. Применяем ConfigMaps и Secrets
echo "Applying ConfigMaps and Secrets..."
kubectl apply -f argo-workflows/configmaps.yaml

# 3. Применяем WorkflowTemplate
echo "Applying WorkflowTemplate..."
kubectl apply -f argo-workflows/cron-workflow.yaml

# 4. (Опционально) Запускаем тестовый Workflow
if [ "$1" == "--test" ]; then
    echo "Running test workflow..."
    argo submit argo-workflows/reports-dag.yaml \
        --namespace=$NAMESPACE \
        -p year=2025 \
        -p report_type=quarter2 \
        --watch
fi

echo "=== Deployment complete ==="
echo ""
echo "Useful commands:"
echo "  argo list -n $NAMESPACE                    # List workflows"
echo "  argo get <workflow-name> -n $NAMESPACE     # Get workflow details"
echo "  argo logs <workflow-name> -n $NAMESPACE    # View logs"
echo "  argo cron list -n $NAMESPACE               # List cron workflows"
echo ""
echo "To run manually:"
echo "  argo submit argo-workflows/reports-dag.yaml -n $NAMESPACE -p year=2025 -p report_type=quarter2"
