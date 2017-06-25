# Implements hack/lib/version.sh's tidb::version::ldflags() for Bazel.
def version_x_defs():
  # This should match the list of packages in kube::version::ldflag
  stamp_pkgs = [
      "github.com/pingcap/tidb/version",
      ]
  # This should match the list of vars in tidb::version::ldflags
  # It should also match the list of vars set in hack/print-workspace-status.sh.
  stamp_vars = [
      "gitVersion",
      "buildDate",
      "gitCommit",
      "gitTreeState",
  ]
  # Generate the cross-product.
  x_defs = {}
  for pkg in stamp_pkgs:
    for var in stamp_vars:
      x_defs["%s.%s" % (pkg, var)] = "{%s}" % var
  return x_defs
