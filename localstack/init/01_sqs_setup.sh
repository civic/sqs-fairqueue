#!/bin/bash

awslocal sqs create-queue --queue-name test-queue --region ap-northeast-1 --attributes VisibilityTimeout=300,MessageRetentionPeriod=600
