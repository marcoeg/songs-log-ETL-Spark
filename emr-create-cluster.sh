#!/bin/sh
aws emr create-cluster --applications Name=Ganglia Name=Spark Name=Zeppelin \
	--ebs-root-volume-size 10 \
	--ec2-attributes '{"KeyName":"spark-cluster","InstanceProfile":"EMR_EC2_DefaultRole","SubnetId":"subnet-d9883282","EmrManagedSlaveSecurityGroup":"sg-0de6ee4bee9b585d4","EmrManagedMasterSecurityGroup":"sg-0e47380bd61d5fe96"}' \
	--service-role EMR_DefaultRole \
	--enable-debugging \
	--release-label emr-5.20.0 \
	--log-uri 's3n://aws-logs-892335585962-us-west-2/elasticmapreduce/' \
	--name 'meg-spark' \
	--instance-groups '[{"InstanceCount":1,"EbsConfiguration":{"EbsBlockDeviceConfigs":[{"VolumeSpecification":{"SizeInGB":32,"VolumeType":"gp2"},"VolumesPerInstance":1}]},"InstanceGroupType":"MASTER","InstanceType":"m5.xlarge","Name":"Master Instance Group"},{"InstanceCount":3,"EbsConfiguration":{"EbsBlockDeviceConfigs":[{"VolumeSpecification":{"SizeInGB":32,"VolumeType":"gp2"},"VolumesPerInstance":1}]},"InstanceGroupType":"CORE","InstanceType":"m5.xlarge","Name":"Core Instance Group"}]' \
	--scale-down-behavior TERMINATE_AT_TASK_COMPLETION \
	--region us-west-2