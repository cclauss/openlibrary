#! /usr/bin/env python3
"""
Create a dump of all Open Library records, and generate sitemaps

To run in local environment:
    docker-compose exec web scripts/oldump.sh $(date +%Y-%m-%d)
Will create files in the OL root at dumps/ , including edits up to today.

Call flow:
docker-compose.production.yml defines `cron-jobs` Docker container.
--> docker/ol-cron-start.sh sets up the cron tasks.
    --> olsystem: /etc/cron.d/openlibrary.ol_home0 defines the actual job
        --> scripts/oldump.sh
            --> scripts/oldump.py
                --> openlibrary/data/dump.py

Testing (stats as of November 2021):
The cron job takes 18+ hours to process 192,000,000+ records in 29GB of data!!

It is therefore essential to test with a subset of the full data so in the file
`openlibrary/data/dump.py`, the `read_data_file()` has a `max_lines` parameter which
can control the size of the subset.  In a production setting, leave `max_lines` set
to zero so that all records are processed.  When testing, set `max_lines` to a more
reasonable number such as 1_000_000.  The first step of this script will still take
110 minutes to extract the 29GB of data from the database so it is highly
recommended to save a copy of data.txt.gz in another directory to accelerate the
testing of subsequent job steps.  See `TESTING:` comments below.

Successful data dumps are transferred to:
    https://archive.org/details/ol_exports?sort=-publicdate
"""

import argparse
import logging
import sys
from datetime import date
from pathlib import Path
from subprocess import run

import _init_path

from infogami import config
from openlibrary.config import load_config
from openlibrary.utils.sentry import Sentry

logger = logging.getLogger(__file__)
logger.setLevel(logging.DEBUG)
# ia_config_file =
# on_openlibrary_host = Path("/1/temp/").exists()
# temp_dir = if Path("/openlibrary/dumps")

# To run a testing subset of the full ol-dump, uncomment the following line.
# export OLDUMP_TESTING=true
r"""
SCRIPTS=/openlibrary/scripts
PSQL_PARAMS=${PSQL_PARAMS:-"-h db openlibrary"}
TMPDIR=${TMPDIR:-/openlibrary/dumps}
OL_CONFIG=${OL_CONFIG:-/openlibrary/conf/openlibrary.yml}

yymm=`date +\%Y-\%m`
yymmdd=$1
archive=$2
overwrite=$3

cdump=ol_cdump_$yymmdd
dump=ol_dump_$yymmdd

if [ $# -lt 1 ]; then
    echo "USAGE: $0 yyyy-mm-dd [--archive] [--overwrite]" 1>&2
    exit 1
fi

function cleanup() {
    rm -f $TMPDIR/data.txt.gz
    rm -rf $TMPDIR/dumps/ol_*

}

function log() {
    echo "* $@" 1>&2
}

function archive_dumps() {
    # Copy data dumps to https://archive.org/details/ol_exports?sort=-publicdate
    # For progress on transfers, see:
    # https://catalogd.archive.org/catalog.php?checked=1&all=1&banner=rsync%20timeout
    # TODO: Switch to ia client tool. This will only work in production 'til then
    log "ia version is v$(ia --version)"  # ia version is v2.2.0
    is_uploaded=$(ia list ${dump} | wc -l)
    if [[ $is_uploaded == 0 ]]
    then
	ia --config-file=/olsystem/etc/ia.ini upload $dump  $dump/  --metadata "collection:ol_exports" --metadata "year:${yymm:0:4}" --metadata "format:Data" --retries 300
	ia --config-file=/olsystem/etc/ia.ini upload $cdump $cdump/ --metadata "collection:ol_exports" --metadata "year:${yymm:0:4}" --metadata "format:Data" --retries 300
    else
	log "Skipping: Archival Zip already exists"
    fi
}

# script <date> --archive --overwrite
log "[$(date)] $0 $1 $2 $3"
log "<host:${HOSTNAME:-$HOST}> <user:$USER> <dir:$TMPDIR>"

if [[ $@ == *'--overwrite'* ]]
then
   log "Cleaning Up: Found --cleanup, removing old files"
   cleanup
fi

# create a clean directory
mkdir -p $TMPDIR/dumps
cd $TMPDIR/dumps

# If there's not already a completed dump for this YY-MM
if [[ ! -d $(compgen -G "ol_cdump_$yymm*") ]]
then

  # Generate Reading Log/Ratings dumps
  if [[ ! -f $(compgen -G "ol_dump_reading-log_$yymm*.txt.gz") ]]
  then
      log "generating reading log table: ol_dump_reading-log_$yymmdd.txt.gz"
      time psql $PSQL_PARAMS --set=upto="$yymmdd" -f $SCRIPTS/dump-reading-log.sql | gzip -c > ol_dump_reading-log_$yymmdd.txt.gz
  else
      log "Skipping: $(compgen -G "ol_dump_reading-log_$yymm*.txt.gz")"
  fi


  if [[ ! -f $(compgen -G "ol_dump_ratings_$yymm*.txt.gz") ]]
  then
      log "generating ratings table: ol_dump_ratings_$yymmdd.txt.gz"
      time psql $PSQL_PARAMS --set=upto="$yymmdd" -f $SCRIPTS/dump-ratings.sql | gzip -c > ol_dump_ratings_$yymmdd.txt.gz
  else
      log "Skipping: $(compgen -G "ol_dump_ratings_$yymm*.txt.gz")"
  fi


  if [[ ! -f "data.txt.gz" ]]
  then
      log "generating the data table: data.txt.gz -- takes approx. 110 minutes..."
      # In production, we copy the contents of our database into the `data.txt.gz` file.
      # else if we are testing, save a lot of time by using a preexisting `data.txt.gz`.
      if [[ -z $OLDUMP_TESTING ]]; then
	  time psql $PSQL_PARAMS -c "copy data to stdout" | gzip -c > data.txt.gz
      fi
  else
      log "Skipping: data.txt.gz"
  fi


  if [[ ! -f $(compgen -G "ol_cdump_$yymm*.txt.gz") ]]
  then
      # generate cdump, sort and generate dump
      log "generating $cdump.txt.gz -- takes approx. 500 minutes for 192,000,000+ records..."
      # if $OLDUMP_TESTING has been exported then `oldump.py cdump` will only process a subset.
      time python $SCRIPTS/oldump.py cdump data.txt.gz $yymmdd | gzip -c > $cdump.txt.gz
      log "generated $(compgen -G "ol_cdump_$yymm*.txt.gz")"
  else
      log "Skipping: $(compgen -G "ol_cdump_$yymm*.txt.gz")"
  fi


  if [[ ! -f $(compgen -G "ol_dump_*.txt.gz") ]]
  then
      echo "generating the dump -- takes approx. 485 minutes for 173,000,000+ records..."
      time gzip -cd $(compgen -G "ol_cdump_$yymm*.txt.gz") | python $SCRIPTS/oldump.py sort --tmpdir $TMPDIR | python $SCRIPTS/oldump.py dump | gzip -c > $dump.txt.gz
      echo "generating $(compgen -G "ol_dump_$yymm*.txt.gz")"
  else
      echo "Skipping: $(compgen -G "ol_dump_$yymm*.txt.gz")"
  fi


  if [[ ! -f $(compgen -G "ol_dump_*_$yymm*.txt.gz") ]]
  then
      mkdir -p $TMPDIR/oldumpsort
      echo "splitting the dump: ol_dump_%s_$yymmdd.txt.gz -- takes approx. 85 minutes for 68,000,000+ records..."
      time gzip -cd $dump.txt.gz | python $SCRIPTS/oldump.py split --format ol_dump_%s_$yymmdd.txt.gz
      rm -rf $TMPDIR/oldumpsort
  else
      echo "Skipping $(compgen -G "ol_dump_*_$yymm*.txt.gz")"
  fi

  mkdir -p $dump $cdump
  mv ol_dump_*.txt.gz $dump
  mv $cdump.txt.gz $cdump

  log "dumps are generated at $PWD"
else
  log "Skipping generation: dumps already exist at $PWD"
fi
ls -lhR

# ========
# Archival
# ========
# Only archive if that caller has requested it and we are not testing.
if [ "$archive" == "--archive" ]; then
    if [[ -z $OLDUMP_TESTING ]]; then
	archive_dumps
    fi
fi

# =================
# Generate Sitemaps
# =================
if [[ ! -d $TMPDIR/sitemaps ]]
then
    log "generating sitemaps"
    mkdir -p $TMPDIR/sitemaps
    cd $TMPDIR/sitemaps
    time python $SCRIPTS/sitemaps/sitemap.py $TMPDIR/dumps/$dump/$dump.txt.gz > sitemaps.log
    rm -fr $TMPDIR/sitemaps
    ls -lh
else
    log "Skipping sitemap"
fi

MSG="$(date): $USER has completed $0 $1 $2 $3 in $TMPDIR on ${HOSTNAME:-$HOST}"
echo $MSG

# remove the dump of data table
# In production, we remove the raw database dump to save disk space.
# else if we are testing, we keep the raw database dump for subsequent test runs.
if [[ -z $OLDUMP_TESTING ]]
then
    echo "deleting the data table dump"
    # After successful run (didn't terminate w/ error)
    # Remove any leftover ol_cdump* and ol_dump* files or directories.
    # Remove the tmp sort dir after dump generation
fi
"""

def parse_args() -> argparse.Namespace:
    """
    usage: oldump.py [-h] [--archive] [--use-existing] yyyy_mm_dd

    Run Open Library's data dumps.

    positional arguments:
      yyyy_mm_dd      last day of the data dump

    optional arguments:
      -h, --help      show this help message and exit
      --archive       write the dump results to the Internet Archive
      --use-existing  use an existing raw database dump
    ---
    oldump.py 1970-02-13
    * Namespace(yyyy_mm_dd='1970-02-13', archive=False, use_existing=False)
    ---
    oldump.py 1970-02-13
    * Namespace(yyyy_mm_dd='1970-02-13', archive=True, use_existing=True)
    """
    parser = argparse.ArgumentParser(description="Run Open Library's data dumps.")
    # TODO: Assume today if yyyy_mm_dd is not provided
    parser.add_argument('yyyy_mm_dd', help='last day of the data dump')
    parser.add_argument(
        '--archive',
        action='store_true',
        help='write the dump results to the Internet Archive'
    )
    parser.add_argument(
        '--use-existing',
        action='store_true',
        help='use an existing raw database dump'
    )
    return parser.parse_args()


def command_line() -> str:
    """
    For debugging parse_args()
       sys.argv = save_argv + ["1970-02-13", "--archive", "--use-existing"]
       print(f"{command_line()}{parse_args()}")
    """
    return f"\n{' '.join([Path(sys.argv[0]).name] + sys.argv[1:])}  # -->\n"

def main() -> int:
    ia_cofig_file = Path("/olsystem/etc/ia.ini")  # ia login creds from olsystem
    working_dir = Path("/1/temp/" ia_cofig_file.exists() else "/openlibrary/dumps")
    args = parse_args()
    yyyy_mm_dd = date.strpstr(args.yyyy_mm_dd, "%Y-%m-%d")
    if args.archive and not ia_cofig_file.exists():
        raise ValueError("'--archive' can only be used on Open Library hosts")
    if args.use_existing and




if __name__ == "__main__":
    print("{}: Python {}.{}.{}".format(__file__, *sys.version_info), file=sys.stderr)
    # print(parse_args())
    save_argv = sys.argv[:]
    sys.argv = save_argv + ["1970-02-13"]
    print(f"{command_line()}{parse_args()}")
    sys.argv = save_argv + ["1970-02-13", "--archive", "--use-existing"]
    print(f"{command_line()}{parse_args()}")
    sys.argv = save_argv + ["--help"]
    print(f"{command_line()}{parse_args()}")
    sys.argv = save_argv
    print(f"{command_line()}{parse_args()}")

#    from openlibrary.data import dump

#    dump.main(sys.argv[1], sys.argv[2:])

    ol_config = os.getenv("OL_CONFIG")
    if ol_config:
        logger.info(f"loading config from {ol_config}")
        load_config(ol_config)
        sentry = Sentry(getattr(config, "sentry_cron_jobs", {}))
        if sentry.enabled:
            sentry.init()

    main(sys.argv[1], sys.argv[2:])