#!/bin/bash

source config.fastberry

watch -n60 'psql -f sql/latest_new_contact.sql'
