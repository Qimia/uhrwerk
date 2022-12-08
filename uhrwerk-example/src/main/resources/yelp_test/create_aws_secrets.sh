#!/usr/bin/env bash

aws secretsmanager create-secret \
    --name "uhrwerk/meta_store/db_user" \
    --secret-string "UHRWERK_USER" \
    --region eu-west-1

aws secretsmanager create-secret \
    --name "uhrwerk/meta_store/db_password" \
    --secret-string "Xq92vFqEKF7TB8H9" \
    --region eu-west-1

aws secretsmanager create-secret \
    --name "uhrwerk/yelp_test/yelp_db_passwd" \
    --secret-string "61ePGqq20u9TZjbNhf0" \
    --region eu-west-1