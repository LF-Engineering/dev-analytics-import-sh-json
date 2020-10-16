# import-sh-json
Import Bitergia identity export JSONs.

# Usage

- Start local MariaDB server via: `PASS=rootpwd ./mariadb_local_docker.sh`.
- Connect to local MariaDB server via: `USR=root PASS=rootpwd ./mariadb_root_shell.sh` to test database connection.
- Initialize SortingHat user & database: `USR=root PASS=rootpwd SH_USR=shusername SH_PASS=shpwd SH_DB=shdb [FULL=1] ./mariadb_init.sh`.
- If `FULL=1` is specified, SortingHat database will be created from gitignored populated `sortinghat.sql` file instead of an empty structure file `structure.sql`.
- To drop SortingHat database & user (just an util script): `USR=root PASS=rootpwd SH_USR=shusername SH_DB=shdb ./mariadb_init.sh`.
- Connect to SortingHat database via: `SH_USR=shusername SH_PASS=shpwd SH_DB=shdb ./mariadb_sortinghat_shell.sh` to test SortingHat database connection.
- Determine the project slug to be used and then specify it via `PROJECT_SLUG=foundation/project`.
- To import data form Bitergia exported JSON files do: `[REPLACE=1] [COMPARE=1] [DEBUG=1] [PROJECT_SLUG=foundation/project] SH_USR=shusername SH_PASS=shpwd SH_DB=shdb SH_PORT=13306 ./import-sh-json file1.json file2.json ...`
- If you specify `REPLACE=1` it will overwrite data on conflicts.  Otherwise it will not add data if there is a conflict.
- If you specify `COMPARE=1` it will check if exacty the same data already exists before attempting to add or replace records.
- Typical usage inside MariaDB K8s pods with all env defined by pod: `REPLACE=1 COMPARE=1 PROJECT_SLUG=foundation/project ./import-sh-json cloudfoundry_sh.json oci_sh.json onap_sh.json opnfv_sh.json yoctoproject_sh.json`.
- Typical usage using external SortingHat DB access: `REPLACE=1 COMPARE=1 PROJECT_SLUG=foundation/project SH_HOST=xyz.us-abcd-N.elb.amazonaws.com SH_USR="`cat ~/dev/LF-Engineering/darst/mariadb/secrets/USER.secret`" SH_PASS="`cat ~/dev/LF-Engineering/darst/mariadb/secrets/PASS.prod.secret`" SH_DB="`cat ~/dev/LF-Engineering/darst/mariadb/secrets/DB.secret`" ./import-sh-json *.json`
- Other example: `[DRY=1] PROJECT_SLUG=foundation/project SYNC_URL="`cat ~/dev/go/src/github.com/LF-Engineering/dev-analytics-affiliation/helm/da-affiliation/secrets/SYNC_URL.prod.secret`" SH_DSN="`cat ~/dev/go/src/github.com/LF-Engineering/dev-analytics-affiliation/helm/da-affiliation/secrets/SH_DB_ENDPOINT.prod.secret`&parseTime=true" PROJECT_SLUG="lfn/onap" ./import-sh-json sh/onap_sh.json`.
- Run locally example: `REPLACE='' COMPARE=1 SH_HOST=127.0.0.1 SH_PORT=13306 SH_DB=sortinghat SH_USR=sortinghat SH_PASS=pwd PROJECT_SLUG=finos ./import-sh-json sh/dump_sh.json`.
- If using manual `SH_DSN` - remember to add option `parseTime=true`.
