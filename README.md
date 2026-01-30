## Overview

Build a production-ready data ingestion system that extracts event data from the DataSync Analytics API and stores it in a PostgreSQL database.

## Requirements

Your solution must:

1. Run entirely in Docker using the provided `docker-compose.yml`
2. Work with the command: `sh run-ingestion.sh`
**Tools Policy:**
- **Allowed:** Any AI coding tools or development tools during development
- **Solution constraint:** Your final solution must run entirely in Docker without requiring external API keys or 3rd party services


If you use AI tools, please document which ones and how they helped in your solution's README.

## The Challenge

DataSync Analytics is a live application with:
- **Dashboard:** http://datasync-dev-alb-101078500.us-east-1.elb.amazonaws.com (explore the UI!)
- **API:** http://datasync-dev-alb-101078500.us-east-1.elb.amazonaws.com/api/v1

Your task is to:

1. **Connect** to the DataSync API
2. **Extract** ALL events from the system (3,000,000)
3. **Handle** the API's pagination correctly
4. **Respect** rate limits
5. **Store** data in PostgreSQL
7. **Make it resumable** (save progress, resume after failure)

### Important Notes

- The API documentation is minimal by design
- Part of this challenge is **discovering** how the API works
- Pay attention to response headers and data formats
- The API has behaviors that aren't documented
- Timestamp formats may vary across responses - normalize carefully

## Getting Started

### Prerequisites

- Docker and Docker Compose
- Node.js 20+
- npm or yarn

### Your Workspace

Use this directory as your workspace. A `docker-compose.yml` is provided with PostgreSQL for your solution.

```bash
docker compose up -d
```

This gives you:
- PostgreSQL at `localhost:5434`

### Exploring the Application

**Dashboard:** http://datasync-dev-alb-101078500.us-east-1.elb.amazonaws.com
- Browse the dashboard to understand the data model
- Curious developers explore everything...

**API Base URL:** `http://datasync-dev-alb-101078500.us-east-1.elb.amazonaws.com/api/v1`

**API Key:** You should have received a unique API key from your interviewer.

> **Important:** Your API key is valid for **3 hours from first use**. The timer starts when you make your first API call. Plan your work accordingly.

**API Documentation:** http://datasync-dev-alb-101078500.us-east-1.elb.amazonaws.com/docs/api.md

## Requirements

### Must Have

1. **TypeScript** codebase
2. **PostgreSQL** for data storage
3. **Docker Compose** for running your solution
4. **Proper error handling** and logging
5. **Rate limit handling** - respect the API limits
6. **Resumable ingestion** - if the process crashes, it should resume from where it left off

### Should Have

1. **Throughput optimization** - maximize events per second
2. **Progress tracking** - show ingestion progress
3. **Health checks** - monitor worker health

### Nice to Have

1. **Unit tests**
2. **Integration tests**
3. **Metrics/monitoring**
4. **Architecture documentation**

## Submitting Your Results

Once you've ingested all events, submit your results to verify completion.

### Step 1: Push Your Solution to GitHub

Before submitting, push your solution to a GitHub repository. This allows us to review your code and see your commit history/progress.

### Step 2: Submit via API

**POST** `http://datasync-dev-alb-101078500.us-east-1.elb.amazonaws.com/api/v1/submissions`

Submit a file containing all event IDs (one per line) along with your GitHub repo URL.

**Headers:**
- `X-API-Key`: Your API key
- `Content-Type`: `text/plain` or `application/json`

**Option 1: Plain text with query param (recommended)**
```bash
curl -X POST \
  -H "X-API-Key: YOUR_API_KEY" \
  -H "Content-Type: text/plain" \
  --data-binary @event_ids.txt \
  "http://datasync-dev-alb-101078500.us-east-1.elb.amazonaws.com/api/v1/submissions?github_repo=https://github.com/yourusername/your-repo"
```

**Option 2: JSON**
```bash
curl -X POST \
  -H "X-API-Key: YOUR_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{
    "ids": "id1\nid2\nid3",
    "githubRepoUrl": "https://github.com/yourusername/your-repo"
  }' \
  http://datasync-dev-alb-101078500.us-east-1.elb.amazonaws.com/api/v1/submissions
```

**Response:**
```json
{
  "success": true,
  "data": {
    "submissionId": "uuid",
    "eventCount": 3000000,
    "githubRepoUrl": "https://github.com/yourusername/your-repo",
    "submittedAt": "2024-01-15T10:30:00.000Z",
    "timeToSubmit": {
      "ms": 1234567,
      "seconds": 1235,
      "minutes": 20.6,
      "formatted": "20m 35s"
    },
    "submissionNumber": 1,
    "remainingSubmissions": 4
  },
  "message": "Submission #1 received with 3,000,000 event IDs. 4 submissions remaining."
}
```

**Limits:**
- Maximum **5 submissions** per API key
- The response includes your completion time (from first API call to submission)

**Check your submissions:**
```bash
curl -H "X-API-Key: YOUR_API_KEY" \
  http://datasync-dev-alb-101078500.us-east-1.elb.amazonaws.com/api/v1/submissions
```

## Important: Verification Testing

**Your solution will be tested after submission to verify it works correctly.**

- The full ingestion must work when running `sh run-ingestion.sh` from scratch on a clean Linux machine using Docker
- We will run your solution on a fresh environment with only Docker installed
- The following do NOT count as valid solutions:
  - WIP/incomplete code that requires manual intervention
  - Solutions that require manual pauses or human interaction during execution
  - Code that needs to be modified after starting the ingestion
  - Solutions that only work after multiple manual restarts

Your solution must be fully automated and complete the entire ingestion without any manual steps.

## What to Submit

Your solution should include:

1. All source code in the `packages/` directory
2. Updated `docker-compose.yml` if needed
3. `README.md` with:
   - How to run your solution
   - Architecture overview
   - Any discoveries about the API
   - What you would improve with more time

## Evaluation Criteria

| Category | Weight |
|----------|--------|
| API Discovery & Throughput | 60% |
| Job Processing Architecture | 40% |

**Your score is primarily based on throughput** - how many events per minute can your solution ingest?

> **Challenge yourself:** Top candidates have solved this entire challenge - including ingesting all 3M events - in under 30 minutes. If you feel limited by the API, keep pushing. There's always a faster way.

## Tips

- Start by exploring the API thoroughly - this is critical
- Make requests, look at responses, **check headers carefully**
- The documented API may not be the fastest way...
- Think about failure scenarios - what happens if the process crashes mid-ingestion?
- Consider how to **maximize throughput** while respecting rate limits
- Good engineers explore every corner of an application
- Cursors have a lifecycle - don't let them get stale

## Questions?

If something is unclear about the requirements (not the API!), please reach out to your contact.

Good luck!
