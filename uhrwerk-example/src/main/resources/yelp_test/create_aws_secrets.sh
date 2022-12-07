#!/usr/bin/env bash

aws secretsmanager create-secret \
    --name "uhrwerk/meta_store/db_user" \
    --secret-string "UHRWERK_USER"

aws secretsmanager create-secret \
    --name "uhrwerk/meta_store/db_password" \
    --secret-string "Xq92vFqEKF7TB8H9"

aws secretsmanager create-secret \
    --name "uhrwerk/yelp/db_password" \
    --secret-string "61ePGqq20u9TZjbNhf0"