# EMR Studio Terraform 구성
resource "aws_emr_studio" "bigdata_studio" {
  name                        = "${var.project_name}-studio"
  auth_mode                  = "IAM"
  default_s3_location        = "s3://${var.s3_bucket_name}/studio/"
  engine_security_group_id   = aws_security_group.emr_studio_engine.id
  service_role               = aws_iam_role.emr_studio_service_role.arn
  subnet_ids                 = data.aws_subnets.default.ids
  vpc_id                     = data.aws_vpc.default.id
  workspace_security_group_id = aws_security_group.emr_studio_workspace.id

  tags = var.tags
}

# EMR Studio 서비스 역할
resource "aws_iam_role" "emr_studio_service_role" {
  name = "${var.project_name}-emr-studio-service-role"

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
}

resource "aws_iam_role_policy_attachment" "emr_studio_service_role_policy" {
  role       = aws_iam_role.emr_studio_service_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/EMRStudioServiceRolePolicy"
}

# 보안 그룹
resource "aws_security_group" "emr_studio_engine" {
  name_prefix = "${var.project_name}-emr-studio-engine-"
  vpc_id      = data.aws_vpc.default.id

  ingress {
    from_port = 18888
    to_port   = 18888
    protocol  = "tcp"
    security_groups = [aws_security_group.emr_studio_workspace.id]
  }

  egress {
    from_port   = 0
    to_port     = 65535
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = var.tags
}

resource "aws_security_group" "emr_studio_workspace" {
  name_prefix = "${var.project_name}-emr-studio-workspace-"
  vpc_id      = data.aws_vpc.default.id

  egress {
    from_port = 18888
    to_port   = 18888
    protocol  = "tcp"
    security_groups = [aws_security_group.emr_studio_engine.id]
  }

  egress {
    from_port   = 443
    to_port     = 443
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = var.tags
}

# 데이터 소스
data "aws_vpc" "default" {
  default = true
}

data "aws_subnets" "default" {
  filter {
    name   = "vpc-id"
    values = [data.aws_vpc.default.id]
  }
}

# 변수
variable "project_name" {
  description = "프로젝트 이름"
  type        = string
  default     = "bigdata-pipeline"
}

variable "s3_bucket_name" {
  description = "S3 버킷 이름"
  type        = string
}

variable "tags" {
  description = "공통 태그"
  type        = map(string)
  default = {
    Environment = "Development"
    Project     = "BigDataPipeline"
  }
}

# 출력
output "emr_studio_id" {
  description = "EMR Studio ID"
  value       = aws_emr_studio.bigdata_studio.id
}

output "emr_studio_url" {
  description = "EMR Studio URL"
  value       = aws_emr_studio.bigdata_studio.url
}
