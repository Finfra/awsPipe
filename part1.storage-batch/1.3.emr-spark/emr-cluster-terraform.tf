terraform {
  required_version = ">= 1.0"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

provider "aws" {
  region = var.region
}

# 기본 VPC 및 서브넷 정보
data "aws_vpc" "default" {
  default = true
}

data "aws_subnets" "default" {
  filter {
    name   = "vpc-id"
    values = [data.aws_vpc.default.id]
  }
}

data "aws_availability_zones" "available" {
  state = "available"
}

# EMR 서비스 역할
resource "aws_iam_role" "emr_service_role" {
  name = "${var.project_name}-emr-service-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "elasticmapreduce.amazonaws.com"
        }
      }
    ]
  })

  tags = var.tags
}

resource "aws_iam_role_policy_attachment" "emr_service_role_policy" {
  role       = aws_iam_role.emr_service_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AmazonElasticMapReduceRole"
}

# EMR EC2 인스턴스 역할
resource "aws_iam_role" "emr_ec2_instance_role" {
  name = "${var.project_name}-emr-ec2-instance-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "ec2.amazonaws.com"
        }
      }
    ]
  })

  tags = var.tags
}

resource "aws_iam_role_policy_attachment" "emr_ec2_instance_role_policy" {
  role       = aws_iam_role.emr_ec2_instance_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AmazonElasticMapReduceforEC2Role"
}

# S3 접근 정책 추가
resource "aws_iam_role_policy" "emr_s3_access" {
  name = "emr-s3-access"
  role = aws_iam_role.emr_ec2_instance_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:DeleteObject",
          "s3:ListBucket",
          "s3:GetBucketLocation"
        ]
        Resource = [
          "arn:aws:s3:::${var.s3_bucket_name}*",
          "arn:aws:s3:::${var.s3_bucket_name}*/*"
        ]
      },
      {
        Effect = "Allow"
        Action = [
          "s3:ListAllMyBuckets"
        ]
        Resource = "*"
      }
    ]
  })
}

resource "aws_iam_instance_profile" "emr_ec2_instance_profile" {
  name = "${var.project_name}-emr-ec2-instance-profile"
  role = aws_iam_role.emr_ec2_instance_role.name
}

# EMR 마스터 보안 그룹
resource "aws_security_group" "emr_master" {
  name_prefix = "${var.project_name}-emr-master"
  vpc_id      = data.aws_vpc.default.id

  # SSH 접근
  ingress {
    from_port   = 22
    to_port     = 22
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]  # 실제 환경에서는 제한 필요
  }

  # Spark UI
  ingress {
    from_port = 20888
    to_port   = 20888
    protocol  = "tcp"
    self      = true
  }

  # Jupyter Notebook
  ingress {
    from_port = 8888
    to_port   = 8888
    protocol  = "tcp"
    self      = true
  }

  # EMR 웹 인터페이스
  ingress {
    from_port = 8088
    to_port   = 8088
    protocol  = "tcp"
    self      = true
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = merge(var.tags, {
    Name = "${var.project_name}-emr-master-sg"
  })
}

# EMR 코어/태스크 보안 그룹
resource "aws_security_group" "emr_slave" {
  name_prefix = "${var.project_name}-emr-slave"
  vpc_id      = data.aws_vpc.default.id

  # 클러스터 내부 통신
  ingress {
    from_port = 0
    to_port   = 0
    protocol  = "-1"
    self      = true
  }

  # 마스터 노드와의 통신
  ingress {
    from_port       = 0
    to_port         = 0
    protocol        = "-1"
    security_groups = [aws_security_group.emr_master.id]
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = merge(var.tags, {
    Name = "${var.project_name}-emr-slave-sg"
  })
}

# EMR 클러스터
resource "aws_emr_cluster" "bigdata_cluster" {
  name          = "${var.project_name}-cluster"
  release_label = var.emr_release_label
  applications  = ["Spark", "Hadoop", "JupyterEnterpriseGateway", "Livy"]

  ec2_attributes {
    subnet_id                         = data.aws_subnets.default.ids[0]
    emr_managed_master_security_group = aws_security_group.emr_master.id
    emr_managed_slave_security_group  = aws_security_group.emr_slave.id
    instance_profile                  = aws_iam_instance_profile.emr_ec2_instance_profile.arn
    key_name                         = var.ec2_key_name
  }

  master_instance_group {
    instance_type = var.master_instance_type
    name          = "Master"

    ebs_config {
      size                 = var.ebs_volume_size
      type                 = "gp3"
      volumes_per_instance = 1
    }
  }

  core_instance_group {
    instance_type  = var.core_instance_type
    instance_count = var.core_instance_count
    name           = "Core"

    ebs_config {
      size                 = var.ebs_volume_size
      type                 = "gp3"
      volumes_per_instance = 1
    }

    # Spot 인스턴스로 비용 절감 (옵션)
    bid_price = "0.30"  # 온디맨드 가격의 약 50%
  }

  service_role = aws_iam_role.emr_service_role.arn

  # Spark 최적화 설정
  configurations_json = jsonencode([
    {
      Classification = "spark-defaults"
      Properties = {
        # S3A 최적화
        "spark.hadoop.fs.s3a.impl"                           = "org.apache.hadoop.fs.s3a.S3AFileSystem"
        "spark.hadoop.fs.s3a.aws.credentials.provider"       = "com.amazonaws.auth.InstanceProfileCredentialsProvider"
        "spark.hadoop.fs.s3a.multipart.size"                = "134217728"  # 128MB
        "spark.hadoop.fs.s3a.multipart.threshold"            = "134217728"
        "spark.hadoop.fs.s3a.fast.upload"                   = "true"
        "spark.hadoop.fs.s3a.fast.upload.buffer"            = "bytebuffer"
        "spark.hadoop.fs.s3a.connection.maximum"            = "20"
        
        # Spark SQL 최적화
        "spark.sql.adaptive.enabled"                         = "true"
        "spark.sql.adaptive.coalescePartitions.enabled"      = "true"
        "spark.sql.adaptive.skewJoin.enabled"               = "true"
        "spark.sql.files.maxPartitionBytes"                 = "134217728"
        
        # 압축 설정
        "spark.sql.parquet.compression.codec"               = "snappy"
        "spark.serializer"                                  = "org.apache.spark.serializer.KryoSerializer"
        
        # 메모리 최적화
        "spark.executor.memory"                             = "4g"
        "spark.executor.cores"                              = "2"
        "spark.executor.memoryFraction"                     = "0.8"
        "spark.sql.shuffle.partitions"                     = "200"
        
        # 동적 할당
        "spark.dynamicAllocation.enabled"                   = "true"
        "spark.dynamicAllocation.minExecutors"              = "1"
        "spark.dynamicAllocation.maxExecutors"              = "10"
        "spark.dynamicAllocation.initialExecutors"          = "2"
      }
    },
    {
      Classification = "spark-env"
      Properties = {}
      Configurations = [
        {
          Classification = "export"
          Properties = {
            "PYSPARK_PYTHON" = "/usr/bin/python3"
          }
        }
      ]
    },
    {
      Classification = "jupyter-s3-conf"
      Properties = {
        "s3.persistence.enabled" = "true"
        "s3.persistence.bucket"  = var.s3_bucket_name
      }
    }
  ])

  log_uri = "s3://${var.s3_bucket_name}/emr-logs/"

  # 자동 종료 설정 (4시간 후)
  auto_termination_policy {
    idle_timeout = 14400
  }

  tags = var.tags
}

# 옵션: Auto Scaling 정책 (태스크 노드용)
resource "aws_emr_instance_group" "task_group" {
  count          = 0  # 필요시 1로 변경
  cluster_id     = aws_emr_cluster.bigdata_cluster.id
  instance_type  = var.core_instance_type
  instance_count = 0
  name           = "Task"

  # Spot 인스턴스 사용
  bid_price = "0.20"

  ebs_config {
    size = var.ebs_volume_size
    type = "gp3"
    volumes_per_instance = 1
  }

  # Auto Scaling 설정
  autoscaling_policy = jsonencode({
    Constraints = {
      MinCapacity = 0
      MaxCapacity = 5
    }
    Rules = [
      {
        Name = "ScaleOutMemoryPercentage"
        Description = "Scale out if YARNMemoryAvailablePercentage is less than 15"
        Action = {
          Market = "SPOT"
          SimpleScalingPolicyConfiguration = {
            AdjustmentType = "CHANGE_IN_CAPACITY"
            ScalingAdjustment = 1
            CoolDown = 300
          }
        }
        Trigger = {
          CloudWatchAlarmDefinition = {
            ComparisonOperator = "LESS_THAN"
            EvaluationPeriods = 1
            MetricName = "YARNMemoryAvailablePercentage"
            Namespace = "AWS/ElasticMapReduce"
            Period = 300
            Statistic = "AVERAGE"
            Threshold = 15.0
            Unit = "PERCENT"
            Dimensions = [
              {
                Key = "JobFlowId"
                Value = aws_emr_cluster.bigdata_cluster.id
              }
            ]
          }
        }
      }
    ]
  })
}
