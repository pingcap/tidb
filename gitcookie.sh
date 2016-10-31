touch ~/.gitcookies
chmod 0600 ~/.gitcookies

git config --global http.cookiefile ~/.gitcookies

tr , \\t <<\__END__ >>~/.gitcookies
go.googlesource.com,FALSE,/,TRUE,2147483647,o,git-shenli.pingcap.com=1/rGvVlvFq_x9rxOmXqQe_rfcrjbOk6NSOHIQKhhsfidM
go-review.googlesource.com,FALSE,/,TRUE,2147483647,o,git-shenli.pingcap.com=1/rGvVlvFq_x9rxOmXqQe_rfcrjbOk6NSOHIQKhhsfidM
__END__

