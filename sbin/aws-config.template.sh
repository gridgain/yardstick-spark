#!/bin/bash
export yardstick_spark_aws_bucket=your-aws-bucket
export yardstick_spark_aws_key=your-aws-key
export yardstick_spark_aws_secret_key=your-aws-secret-key
export YARD_SPARK_DIR=/root/yardstick-spark

cat $YARD_SPARK_DIR/config/spark-aws-config.template.xml | sed s"/yardstick_spark_aws_bucket/$yardstick_spark_aws_bucket/" | sed s"/yardstick_spark_aws_key/$yardstick_spark_aws_key/" | sed s"~yardstick_spark_aws_secret_key~$yardstick_spark_aws_secret_key~" > $YARD_SPARK_DIR/config/spark-aws-config.xml 

