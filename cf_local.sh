#!/bin/bash
ORGS_RO=1 MISSING_ORGS_CSV=missing.csv ORGS_MAP_FILE=../dev-analytics-affiliation/map_org_names.yaml REPLACE='' COMPARE=1 PROJECT_SLUG=cloud-foundry-f SH_DSN="`cat ../da-ds-gha/DB_CONN.local.secret`" ./import-sh-json sh/cloudfoundry_sh.json
