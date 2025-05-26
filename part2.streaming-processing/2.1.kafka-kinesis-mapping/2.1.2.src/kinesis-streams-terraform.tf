        # 스케일링 로직
        per_shard_throughput = avg_mb_per_sec / current_shards
        
        print(f"현재 샤드 수: {current_shards}")
        print(f"평균 처리량: {avg_mb_per_sec:.2f} MB/초")
        print(f"샤드당 처리량: {per_shard_throughput:.2f} MB/초")
        
        # 스케일 아웃 조건: 샤드당 처리량이 0.8MB/초 초과
        if per_shard_throughput > 0.8:
            new_shard_count = min(current_shards * 2, 100)  # 최대 100개 제한
            
            print(f"스케일 아웃 필요: {current_shards} -> {new_shard_count}")
            
            kinesis.update_shard_count(
                StreamName=stream_name,
                TargetShardCount=new_shard_count,
                ScalingType='UNIFORM_SCALING'
            )
            
            return {
                'statusCode': 200,
                'body': f'Scaled out from {current_shards} to {new_shard_count} shards'
            }
        
        # 스케일 인 조건: 샤드당 처리량이 0.2MB/초 미만이고 샤드가 2개 이상
        elif per_shard_throughput < 0.2 and current_shards > 1:
            new_shard_count = max(current_shards // 2, 1)
            
            print(f"스케일 인 필요: {current_shards} -> {new_shard_count}")
            
            kinesis.update_shard_count(
                StreamName=stream_name,
                TargetShardCount=new_shard_count,
                ScalingType='UNIFORM_SCALING'
            )
            
            return {
                'statusCode': 200,
                'body': f'Scaled in from {current_shards} to {new_shard_count} shards'
            }
        
        else:
            print("스케일링 불필요")
            return {
                'statusCode': 200,
                'body': 'No scaling needed'
            }
            
    except Exception as e:
        print(f"오류 발생: {str(e)}")
        return {
            'statusCode': 500,
            'body': f'Error: {str(e)}'
        }
EOF
    filename = "index.py"
  }
}

# 애플리케이션 Auto Scaling 설정 (온디맨드 모드가 아닌 경우)
resource "aws_appautoscaling_target" "kinesis_target" {
  count              = var.stream_mode == "PROVISIONED" ? 1 : 0
  max_capacity       = var.max_shard_count
  min_capacity       = var.min_shard_count
  resource_id        = "stream/${aws_kinesis_stream.main_stream.name}"
  scalable_dimension = "kinesis:shard:count"
  service_namespace  = "kinesis"

  tags = var.tags
}

# Auto Scaling 정책 - 스케일 아웃
resource "aws_appautoscaling_policy" "kinesis_scale_out" {
  count              = var.stream_mode == "PROVISIONED" ? 1 : 0
  name               = "${var.project_name}-kinesis-scale-out"
  policy_type        = "TargetTrackingScaling"
  resource_id        = aws_appautoscaling_target.kinesis_target[0].resource_id
  scalable_dimension = aws_appautoscaling_target.kinesis_target[0].scalable_dimension
  service_namespace  = aws_appautoscaling_target.kinesis_target[0].service_namespace

  target_tracking_scaling_policy_configuration {
    target_value = 70.0  # 70% 사용률 유지

    predefined_metric_specification {
      predefined_metric_type = "KinesisStreamIncomingRecords"
    }

    scale_out_cooldown  = 300  # 5분
    scale_in_cooldown   = 300  # 5분
  }
}

# Kinesis Analytics 애플리케이션 (실시간 분석용)
resource "aws_kinesisanalyticsv2_application" "realtime_analytics" {
  name                   = "${var.project_name}-realtime-analytics"
  runtime_environment    = "FLINK-1_13"
  service_execution_role = aws_iam_role.analytics_execution_role.arn

  application_configuration {
    application_code_configuration {
      code_content_type = "PLAINTEXT"
      code_content {
        text_content = <<EOF
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kinesis.FlinkKinesisConsumer;
import org.apache.flink.streaming.connectors.kinesis.config.ConsumerConfigConstants;

import java.util.Properties;

public class StreamingAnalytics {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties kinesisConsumerConfig = new Properties();
        kinesisConsumerConfig.setProperty(ConsumerConfigConstants.AWS_REGION, "${var.region}");
        kinesisConsumerConfig.setProperty(ConsumerConfigConstants.STREAM_INITIAL_POSITION, "LATEST");

        DataStream<String> kinesis = env.addSource(new FlinkKinesisConsumer<>(
            "${aws_kinesis_stream.main_stream.name}",
            new SimpleStringSchema(),
            kinesisConsumerConfig));

        kinesis.print();

        env.execute("Kinesis Analytics");
    }
}
EOF
      }
    }

    flink_application_configuration {
      checkpoint_configuration {
        configuration_type = "DEFAULT"
      }

      monitoring_configuration {
        configuration_type = "DEFAULT"
        log_level         = "INFO"
        metrics_level     = "APPLICATION"
      }

      parallelism_configuration {
        configuration_type = "DEFAULT"
        parallelism       = 1
        parallelism_per_kpu = 1
        auto_scaling_enabled = true
      }
    }
  }

  tags = var.tags
}

# Kinesis Analytics 실행 역할
resource "aws_iam_role" "analytics_execution_role" {
  name = "${var.project_name}-analytics-execution-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "kinesisanalytics.amazonaws.com"
        }
      }
    ]
  })

  tags = var.tags
}

# Analytics 역할 정책
resource "aws_iam_role_policy" "analytics_policy" {
  name = "analytics-kinesis-access"
  role = aws_iam_role.analytics_execution_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "kinesis:DescribeStream",
          "kinesis:GetShardIterator",
          "kinesis:GetRecords",
          "kinesis:ListShards"
        ]
        Resource = aws_kinesis_stream.main_stream.arn
      },
      {
        Effect = "Allow"
        Action = [
          "kms:Decrypt"
        ]
        Resource = aws_kms_key.kinesis_key.arn
      },
      {
        Effect = "Allow"
        Action = [
          "logs:CreateLogGroup",
          "logs:CreateLogStream",
          "logs:PutLogEvents",
          "logs:DescribeLogGroups",
          "logs:DescribeLogStreams"
        ]
        Resource = "*"
      }
    ]
  })
}
