#!/bin/bash
ORGS_RO=1 MISSING_ORGS_CSV=finos_missing.csv ORGS_MAP_FILE=../dev-analytics-affiliation/map_org_names.yaml REPLACE='' COMPARE=1 PROJECT_SLUG=finos-f SH_DSN="`cat ../da-ds-gha/DB_CONN.local.secret`" ./import-sh-json sh/symphonyoss_200605_sh.json
