output "emr_cluster_id" {
  description = "EMR cluster ID"
  value       = aws_emr_cluster.bigdata_cluster.id
}

output "emr_cluster_name" {
  description = "EMR cluster name"
  value       = aws_emr_cluster.bigdata_cluster.name
}

output "emr_master_public_dns" {
  description = "EMR master node public DNS"
  value       = aws_emr_cluster.bigdata_cluster.master_public_dns
}

output "emr_log_uri" {
  description = "EMR log URI in S3"
  value       = aws_emr_cluster.bigdata_cluster.log_uri
}

output "spark_ui_url" {
  description = "Spark UI URL (requires port forwarding or VPN)"
  value       = "http://${aws_emr_cluster.bigdata_cluster.master_public_dns}:20888"
}

output "jupyter_url" {
  description = "Jupyter Enterprise Gateway URL"
  value       = "http://${aws_emr_cluster.bigdata_cluster.master_public_dns}:8888"
}

output "emr_security_groups" {
  description = "EMR security group IDs"
  value = {
    master = aws_security_group.emr_master.id
    slave  = aws_security_group.emr_slave.id
  }
}

output "ssh_command" {
  description = "SSH command to connect to EMR master node"
  value       = "ssh -i ${var.ec2_key_name}.pem hadoop@${aws_emr_cluster.bigdata_cluster.master_public_dns}"
}

output "spark_submit_example" {
  description = "Example spark-submit command"
  value       = "spark-submit --master yarn --deploy-mode cluster s3://${var.s3_bucket_name}/scripts/your-job.py"
}
