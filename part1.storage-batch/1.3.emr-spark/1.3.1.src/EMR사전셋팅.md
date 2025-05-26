
# EMR Role생성
## 1. IAM 콘솔에서 생성하는 방법
  * 오른쪽의 **\[Create a service role]** 클릭
  * 자동으로 필요한 권한(`AmazonElasticMapReduceRole` 등)이 설정된 IAM 역할을 생성함
  * 생성되면 해당 역할을 드롭다운에서 선택 가능

## 2. 수동 생성하는 방법 (터미널 또는 콘솔)
```bash
cd 1.3.1.src
# 기본 EMR 서비스 역할 생성 (필요 시)
aws iam create-role \
  --role-name EMR_DefaultRole \
  --assume-role-policy-document file://trust-policy.json

# 권한 부여
aws iam attach-role-policy \
  --role-name EMR_DefaultRole \
  --policy-arn arn:aws:iam::aws:policy/service-role/AmazonElasticMapReduceRole

```
# EC2 instance profile이 없을 때 처리 방법

## 1. IAM 콘솔에서 생성하는 방법

1. **"Create an instance profile"** 버튼 클릭
2. AWS에서 `EMR_EC2_DefaultRole` 이름으로 생성됨
3. 자동으로 아래 정책이 연결됨:

   * `AmazonElasticMapReduceforEC2Role`
   * `AmazonS3ReadOnlyAccess` 또는 비슷한 S3 접근 권한 정책

## 2. CLI로 수동 생성

```bash
# 1. EC2 인스턴스용 IAM 역할 생성
aws iam create-role \
  --role-name EMR_EC2_DefaultRole \
  --assume-role-policy-document file://emr-ec2-trust-policy.json

# 2. 정책 연결 (S3 접근 + EMR EC2 기능)
aws iam attach-role-policy \
  --role-name EMR_EC2_DefaultRole \
  --policy-arn arn:aws:iam::aws:policy/service-role/AmazonElasticMapReduceforEC2Role

aws iam attach-role-policy \
  --role-name EMR_EC2_DefaultRole \
  --policy-arn arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess

# 3. 인스턴스 프로파일 생성
aws iam create-instance-profile \
  --instance-profile-name EMR_EC2_DefaultRole

# 4. 역할을 인스턴스 프로파일에 연결
aws iam add-role-to-instance-profile \
  --instance-profile-name EMR_EC2_DefaultRole \
  --role-name EMR_EC2_DefaultRole
```

* `emr-ec2-trust-policy.json` 예시:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": {
        "Service": "ec2.amazonaws.com"
      },
      "Action": "sts:AssumeRole"
    }
  ]
}
```

---

# 요약

| 항목      | 설명                                              |
| --------- | ------------------------------------------------- |
| 역할 이름 | `EMR_EC2_DefaultRole` 권장                        |
| 필수 정책 | `AmazonElasticMapReduceforEC2Role` + S3 접근 정책 |
| 사용 위치 | EMR 클러스터 생성 시 "Instance profile"로 선택    |


