## Railway Deployment (Step-by-step)

### 1) Git Init + Push to GitHub
Run these locally in the project folder:

```powershell
git init
git add .
git commit -m "Prepare for Railway deployment"
```

Create a new repo on GitHub, then:

```powershell
git branch -M main
git remote add origin <YOUR_GITHUB_REPO_URL>
git push -u origin main
```

### 2) Railway Project
1. Go to Railway → New Project → Deploy from GitHub.
2. Select your repo.
3. Add **PostgreSQL** plugin.

### 3) Environment Variables
Add in Railway → Variables:
- `SECRET_KEY` = strong random string
- `DATABASE_URL` (auto from PostgreSQL plugin)

### 4) Start Command
Railway will detect:
- `Procfile` → `web: gunicorn app:app --bind 0.0.0.0:$PORT`

### 5) Migrate Local SQLite Data (Optional)
If you need existing data:
1. Export SQLite data and import into Postgres (manual or via tools like pgloader).
2. Once imported, app will use Postgres via `DATABASE_URL`.

### 6) Uploads / Attachments
Uploads are stored under `static/uploads/`.
For persistence:
- Use Railway Volume and mount it to the same path:
  `/app/static/uploads`
This keeps files after redeploys.

### 7) Open App
Use the Railway provided URL.
