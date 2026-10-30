# Branch protection

No git remote is configured yet (this repo was `git init`'d locally per
Phase 0). Branch protection is a GitHub-side setting and needs a remote to
apply to. Once a remote exists, run:

```bash
gh repo create <org>/<repo> --source=. --private
gh api repos/<org>/<repo>/branches/main/protection -X PUT \
  -f required_pull_request_reviews.required_approving_review_count=1 \
  -f enforce_admins=true \
  -f required_status_checks.strict=true \
  -F required_status_checks.contexts[]=secret-scan \
  -F required_status_checks.contexts[]=check-protected-paths \
  -f restrictions=null
```

This requires `gitleaks` (secret-scan.yml) and `check-protected-paths`
(protected-paths.yml) to pass before merge, and blocks force-pushes to
`main` even for admins.
