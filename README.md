# CCDI cBioPortal Database Actions

A set of Prefect workflows that can either create MySQL database dumps and upload them to AWS S3 or restore database dumps from S3 to another database. Restore actions are forthcoming. 

## Overview

This project provides a workflow to:
1. Connect to a MySQL database
2. Create a database dump using mysqldump
3. Upload the dump file to an AWS S3 bucket

## Prerequisites

- Python 3.9+
- MySQL client tools (mysqldump)
- AWS credentials with access to:
  - AWS Secrets Manager
  - S3 bucket

## Installation

1. Clone this repository
2. Install dependencies:
```bash
pip install -r requirements.txt
```

## Configuration

The workflow requires the following:
- AWS credentials configured (either through environment variables or AWS CLI)
- A secret stored in AWS Secrets Manager with the following structure:
```json
{
    "host": "your-database-host",
    "user": "your-database-user",
    "password": "your-database-password",
    "database": "your-database-name",
    "port": 3306
}
```

## Overview

The workflow will:
1. Retrieve database credentials from AWS Secrets Manager
2. Create a database dump with timestamp in the filename
3. Upload the dump file to the specified S3 bucket

## Environment Variables

- `AWS_REGION`: AWS region (default: us-east-1)
- `AWS_SECRET_NAME`: Name of the secret in AWS Secrets Manager (default: test/db/creds)
- `S3_BUCKET`: Name of the S3 bucket for uploads

## Error Handling

The workflow includes error handling for:
- Database connection issues
- mysqldump failures
- S3 upload failures
- AWS Secrets Manager access issues

## Output

- Database dumps are stored to mounted  in the `./dumps` directory
- Uploaded files are stored in the S3 bucket under the `dump_folder/` prefix
- Each dump file is named with the pattern: `{database}_dump_{timestamp}.sql`
- Database dump is then removed from local space

