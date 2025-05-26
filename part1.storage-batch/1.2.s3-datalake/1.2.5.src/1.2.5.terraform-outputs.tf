output "s3_bucket_name" {
  description = "Name of the S3 bucket"
  value       = aws_s3_bucket.datalake.bucket
}

output "s3_bucket_arn" {
  description = "ARN of the S3 bucket"
  value       = aws_s3_bucket.datalake.arn
}

output "s3_bucket_region" {
  description = "Region of the S3 bucket"
  value       = aws_s3_bucket.datalake.region
}

output "s3_bucket_domain_name" {
  description = "Domain name of the S3 bucket"
  value       = aws_s3_bucket.datalake.bucket_domain_name
}

output "sample_s3_paths" {
  description = "Sample S3 paths for data organization"
  value = {
    raw_data      = "s3://${aws_s3_bucket.datalake.bucket}/raw-data/"
    refined_data  = "s3://${aws_s3_bucket.datalake.bucket}/refined-data/"
    curated_data  = "s3://${aws_s3_bucket.datalake.bucket}/curated-data/"
    logs          = "s3://${aws_s3_bucket.datalake.bucket}/logs/"
  }
}
