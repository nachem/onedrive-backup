# OneDrive Backup: AWS ECS Fargate Spot Deployment Guide

This guide provides complete, step-by-step infrastructure-as-code instructions to deploy a cost-optimized, containerized backup system on AWS using the CLI. The setup is designed for large-scale data (2TB+) with high efficiency and minimal overhead.

---

## System Architecture

- **Compute:** AWS ECS Fargate Spot (70–90% discount compared to On-Demand)
- **Scheduling:** EventBridge Scheduler with a 4-hour flexible window to maximize Spot availability
- **Storage:** Amazon S3 with Intelligent-Tiering for automated cost savings
- **Networking:** AWS VPC (Public Subnets) with same-region data transfer optimization

---

## 1. Container Registry (ECR) Setup

Create a private repository and push your backup Docker image to AWS ECR.

```bash
# 1. Create the repository
aws ecr create-repository --repository-name onedrive-backup-task

# 2. Login (Replace <account_id> and <region>)
aws ecr get-login-password --region <region> | \
  docker login --username AWS --password-stdin <account_id>.dkr.ecr.<region>.amazonaws.com

# 3. Build, Tag, and Push
# (Replace <account_id> and <region> in the tag)
docker build -t onedrive-backup-task:latest .
docker tag onedrive-backup-task:latest <account_id>.dkr.ecr.<region>.amazonaws.com/onedrive-backup-task:latest
docker push <account_id>.dkr.ecr.<region>.amazonaws.com/onedrive-backup-task:latest
```

---

## 2. Infrastructure Roles (IAM)

The system requires specific permissions for the container to execute and for the scheduler to trigger tasks.

### A. ECS Task Execution Role

Allows ECS to pull the image from ECR and stream logs to CloudWatch.

```bash
aws iam create-role --role-name OneDriveTaskExecutionRole --assume-role-policy-document ' {
  "Version": "2012-10-17",
  "Statement": [{
    "Effect": "Allow",
    "Principal": {"Service": "ecs-tasks.amazonaws.com"},
    "Action": "sts:AssumeRole"
  }]
}'
```

### B. Scheduler Role

Allows EventBridge to start tasks on your behalf.

```bash
aws iam create-role --role-name OneDriveSchedulerRole --assume-role-policy-document ' {
  "Version": "2012-10-17",
  "Statement": [{
    "Effect": "Allow",
    "Principal": {"Service": "scheduler.amazonaws.com"},
    "Action": "sts:AssumeRole"
  }]
}'
```

---

## 3. ECS Cluster & Logging

```bash
# Create the Log Group for troubleshooting
aws logs create-log-group --log-group-name "/ecs/onedrive-backup"

# Create the Cluster
aws ecs create-cluster --cluster-name onedrive-backup-cluster

# Configure FARGATE_SPOT as the default capacity provider
aws ecs put-cluster-capacity-providers \
  --cluster onedrive-backup-cluster \
  --capacity-providers FARGATE FARGATE_SPOT \
  --default-capacity-provider-strategy capacityProvider=FARGATE_SPOT,weight=1
```

---

## 4. Task Definition

Registers the container details (optimized for 256 CPU / 512 MB RAM).

```bash
aws ecs register-task-definition --family onedrive-backup-task \
  --requires-compatibilities FARGATE \
  --network-mode awsvpc \
  --cpu "256" \
  --memory "512" \
  --execution-role-arn arn:aws:iam::<account_id>:role/OneDriveTaskExecutionRole \
  --container-definitions '[{
    "name": "backup-container",
    "image": "<account_id>.dkr.ecr.<region>.amazonaws.com/onedrive-backup-task:latest",
    "essential": true,
    "logConfiguration": {
      "logDriver": "awslogs",
      "options": {
        "awslogs-group": "/ecs/onedrive-backup",
        "awslogs-region": "<region>",
        "awslogs-stream-prefix": "ecs"
      }
    }
  }]'
```

---

## 5. Daily Schedule (EventBridge)

This triggers at 02:00 AM (Jerusalem Time) with a 4-hour flexible window. This window allows AWS to wait for available Spot capacity, significantly reducing the chance of "Capacity Unavailable" errors.

```bash
aws scheduler create-schedule --name "OneDriveDailyBackup" \
  --schedule-expression "cron(0 2 * * ? *)" \
  --schedule-expression-timezone "Asia/Jerusalem" \
  --flexible-time-window Mode=FLEXIBLE,MaximumWindowInMinutes=240 \
  --target '{
    "Arn": "arn:aws:ecs:<region>:<account_id>:cluster/onedrive-backup-cluster",
    "RoleArn": "arn:aws:iam::<account_id>:role/OneDriveSchedulerRole",
    "RetryPolicy": {"MaximumRetryAttempts": 0},
    "EcsParameters": {
      "TaskDefinitionArn": "arn:aws:ecs:<region>:<account_id>:task-definition/onedrive-backup-task",
      "TaskCount": 1,
      "CapacityProviderStrategy": [{"capacityProvider": "FARGATE_SPOT", "weight": 1}],
      "NetworkConfiguration": {
        "awsvpcConfiguration": {
          "Subnets": ["<subnet_id>"],
          "AssignPublicIp": "ENABLED"
        }
      }
    }
  }'
```

---

## 6. Cost Estimation & Regional Optimization

| Component         | Usage Detail                                 | Estimated Monthly Cost |
| ----------------- | -------------------------------------------- | ---------------------- |
| S3 Storage        | 2 TB in Intelligent-Tiering (Archive Access) | ~$8.00 - $10.00        |
| Compute (Fargate) | ~1hr Daily Run (0.25 vCPU / 0.5 GB RAM)      | ~$0.11                 |
| Data Transfer     | ECS Task to S3 (Same Region)                 | $0.00 (FREE)           |
| **Total**         |                                              | **~$10.11 / Month**    |

**The "Same-Region" Rule:**

- Ensure your S3 Bucket and ECS Cluster are in the same region (e.g., `us-east-1`).
- Data transfer between ECS and S3 in the same region is free of charge.
- Cross-region transfer incurs $0.02/GB. For 100 GB monthly sync, that's $2.00/month extra.
- Same-region transfers are faster and reduce compute costs.

---

## 7. Monitoring CLI Tools

Add this function to your `~/.bashrc` for quick cost health checks:

```bash
onedrive-cost() {
  echo "Service Costs breakdown (Last 30 Days):"
  aws ce get-cost-and-usage \
    --time-period Start=$(date -d "30 days ago" +%Y-%m-%d),End=$(date +%Y-%m-%d) \
    --granularity MONTHLY \
    --metrics "UnblendedCost" \
    --group-by Type=DIMENSION,Key=SERVICE \
    --query "ResultsByTime[0].Groups[?Metrics.UnblendedCost.Amount!=\`0\`].[Keys[0], Metrics.UnblendedCost.Amount]" \
    --output table
}
```

---

## Manual Trigger (Emergency Backup)

If you need to run the backup immediately outside of the schedule:

```bash
aws ecs run-task \
  --cluster onedrive-backup-cluster \
  --task-definition onedrive-backup-task \
  --capacity-provider-strategy capacityProvider=FARGATE_SPOT,weight=1 \
  --network-configuration "awsvpcConfiguration={subnets=[<subnet_id>],assignPublicIp=ENABLED}"
```

---

## Notes

- Replace all `<account_id>`, `<region>`, and `<subnet_id>` placeholders with your actual AWS values.
- For large-scale or production deployments, review AWS best practices for IAM, VPC, and S3 security.
- For more details, see the official AWS documentation for each service referenced above.
