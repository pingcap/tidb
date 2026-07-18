# update tags
git fetch --tags

getLatestTags() {
  release_5_branch_regex="^release-5\.[0-9].*$"
  release_4_branch_regex="^release-4\.[0-9].*$"
  TOTAL_TAGS=$(git for-each-ref --sort=creatordate  refs/tags | awk -F '/' '{print $3}')
  # we should filter such tags
  # v5.0.2-20210628
  # v5.0.2-alpha
  # because these tags don't have corresponding docker images.
  filter='-'
  # latest tags
  TAGS=$(echo $TOTAL_TAGS | tr ' ' '\n' | grep -v $filter | tail -n3)
  if git rev-parse --abbrev-ref HEAD | egrep -q $release_5_branch_regex
  then
    # If we are in release-5.0 branch, try to use latest 3 version of 5.x and last 4.x version
    TAGS=$(echo $TOTAL_TAGS | tr ' ' '\n' | fgrep "v4." | grep -v $filter | tail -n1 && echo $TOTAL_TAGS | tr ' ' '\n' | fgrep "v5." | grep -v $filter | tail -n3)
  elif git rev-parse --abbrev-ref HEAD | egrep -q $release_4_branch_regex
  then
    # If we are in release-4.0 branch, try to use latest 3 version of 4.x
    TAGS=$(echo $TOTAL_TAGS | tr ' ' '\n' | fgrep "v4." | grep -v $filter | tail -n3)
  fi
}
