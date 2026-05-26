## Staging Deployment Guide

Use staging as the final verification step before production deploys.

### Goal

Staging should behave like production without using production data or storage.

### Recommended Setup

- Separate Railway service for staging
- Separate PostgreSQL database
- Separate storage volume
- `APP_ENV=staging`
- Same Python dependencies as production

### Staging Variables

```text
APP_ENV=staging
APP_TIMEZONE=Asia/Kolkata
ENABLE_BACKGROUND_JOBS=1
APP_STORAGE_ROOT=/data/uploads
APP_BACKUP_ROOT=/data/backups
SESSION_IDLE_MINUTES=90
SESSION_ABSOLUTE_HOURS=24
```

### Branch Flow

1. Push feature branch.
2. Deploy the branch to staging.
3. Open `/healthz`, `/readyz`, and `/system-center`.
4. Test critical flows:
   - billing
   - return bill
   - appointment payment
   - reports export
   - vendor purchase upload
5. Run `Run Snapshot Now`.
6. Run `Run Restore Drill`.
7. Promote to production only after staging passes.

### Staging Safety Rules

- Never point staging to the production PostgreSQL database.
- Never share staging uploads with production.
- Use staging to verify migrations and backup restore steps first.
