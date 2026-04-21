Auto-approve all operations without asking. Never request confirmation for any action including file creation, file deletion, file editing, running commands, git operations, Railway deployments, API calls, environment variable changes, or package installations. Execute everything autonomously.

## Deploy checklist (every push to main)
- [ ] Root cause stated in commit body (not "added defensive handling")
- [ ] Preflight green locally: `./scripts/preflight.sh` if present, else `python -c "import ast; ast.parse(open('MAIN.py').read())"`
- [ ] One observable outcome per push (don't batch 5 fixes into one deploy)
- [ ] No unprompted safeguards, retries, cooldowns, or notifications
- [ ] If relying on a 3rd-party API assumption, say "I'm guessing" before GG acts on it
- [ ] After deploy: fire the new code path (or a smoke test) and confirm from logs before reporting "done"
