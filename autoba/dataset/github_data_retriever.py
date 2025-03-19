import requests
import time
from datetime import datetime, timedelta
import csv
import os

# GitHub API Configuration
GITHUB_TOKEN = os.getenv("GITHUB_PAT")
if not GITHUB_TOKEN:
    raise ValueError("GitHub PAT is not set. Please set the GITHUB_PAT environment variable.")

REPO_OWNER = "rails"
REPO_NAME = "rails"
BASE_URL = f"https://api.github.com/repos/{REPO_OWNER}/{REPO_NAME}"
HEADERS = {
    "Authorization": f"token {GITHUB_TOKEN}",
    "Accept": "application/vnd.github.v3+json"
}

# Fetch months from environment variable, default to 4 if not set
FETCH_MONTHS = int(os.getenv("FETCH_MONTHS", 4))
FETCH_START_DATE = datetime.now() - timedelta(days=FETCH_MONTHS * 30)

# CSV Filenames
ISSUE_CSV = "issue-details.csv"
PR_CSV = "related-pr-details.csv"

# Ensure CSV files exist with headers
def initialize_csv():
    if not os.path.exists(ISSUE_CSV):
        with open(ISSUE_CSV, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(["issueId", "issueCreatorLoginId", "createdDate", "closedDate", "title", "description"])

    if not os.path.exists(PR_CSV):
        with open(PR_CSV, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(["issueId", "prId", "prTitle", "prDescription", "prStatus", "prFilePaths", "prCreatorLoginId", "prCreatedDate", "prMergedDate"])

def get_last_saved_issue_info():
    """Gets the last saved issue's number and closed_at date to resume from that point."""
    if not os.path.exists(ISSUE_CSV):
        return None, None

    with open(ISSUE_CSV, "r", encoding="utf-8") as f:
        lines = f.readlines()
        if len(lines) < 2:
            return None, None  # No issues saved yet

    last_line = lines[-1].strip().split(",")
    last_issue_id = int(last_line[0])
    last_closed_at = last_line[3]  # Closed date in ISO format
    return last_issue_id, last_closed_at

def get_closed_issues():
    """Fetch closed issues from GitHub since the last recorded closed issue date, filtering by FETCH_MONTHS."""
    last_saved_issue, last_closed_at = get_last_saved_issue_info()

    # Use the last saved closed date if available, but not older than FETCH_MONTHS range
    # Convert GitHub timestamps (with Z) into a Python datetime object
    if last_closed_at:
        last_closed_at = datetime.strptime(last_closed_at, "%Y-%m-%dT%H:%M:%SZ")
        if last_closed_at > FETCH_START_DATE:
            since_date = last_closed_at.isoformat()
        else:
            since_date = FETCH_START_DATE.isoformat()
    else:
        since_date = FETCH_START_DATE.isoformat()

    issues = []
    page = 1

    while True:
        url = f"{BASE_URL}/issues?state=closed&since={since_date}&per_page=100&page={page}"
        response = requests.get(url, headers=HEADERS)

        if response.status_code != 200:
            print(f"Error fetching issues: {response.json()}")
            break

        data = response.json()
        if not data:
            break  # No more issues to fetch

        for issue in data:
            if "pull_request" in issue:  # Exclude PRs
                continue

            closed_at = issue.get("closed_at")
            if not closed_at:
                continue

            closed_at_date = datetime.strptime(closed_at, "%Y-%m-%dT%H:%M:%SZ")

            # Skip issues that were closed before the allowed range
            if closed_at_date < FETCH_START_DATE:
                print(f"Skipping Issue #{issue['number']} - Closed on {closed_at_date}")
                continue

            issue_id = issue["number"]

            # Skip already saved issues
            if last_saved_issue and issue_id <= last_saved_issue:
                continue

            issues.append({
                "id": issue_id,
                "creator": issue["user"]["login"],
                "created_at": issue["created_at"],
                "closed_at": closed_at,
                "title": issue["title"].replace("\n", " ").replace(",", " "),
                "body": (issue["body"] or "").replace("\n", " ").replace(",", " "),
            })
            save_issue_to_csv(issues[-1])  # Save immediately

        print(f"Fetched {len(issues)} issues from page {page}")
        page += 1
        time.sleep(2)  # Avoid hitting rate limits

    return issues

def get_linked_pull_requests(issue_number):
    """Fetch linked pull requests from the same repository for a given issue."""
    linked_prs = []

    # Step 1: Check timeline for cross-referenced PRs
    timeline_url = f"{BASE_URL}/issues/{issue_number}/timeline"
    response = requests.get(timeline_url, headers=HEADERS)

    if response.status_code == 200:
        for event in response.json():
            if event.get("event") == "cross-referenced" and "source" in event:
                source = event["source"]
                if "issue" in source and "pull_request" in source["issue"]:
                    pr = source["issue"]
                    linked_prs.append(extract_pr_details(issue_number, pr))

    # Step 2: Check issue events for referenced PRs
    events_url = f"{BASE_URL}/issues/{issue_number}/events"
    response = requests.get(events_url, headers=HEADERS)

    if response.status_code == 200:
        for event in response.json():
            if event.get("event") == "referenced" and "commit_id" in event:
                pr_number = get_pr_number_from_commit(event["commit_id"])
                if pr_number:
                    linked_prs.append(fetch_pr_details(issue_number, pr_number))

    # Save each PR entry immediately to CSV
    for pr_data in linked_prs:
        save_pr_to_csv(pr_data)

    return linked_prs

def extract_pr_details(issue_id, pr):
    """Extract PR details from GitHub API response."""
    return {
        "issueId": issue_id,
        "prId": pr["number"],
        "prTitle": pr["title"].replace("\n", " ").replace(",", " "),
        "prDescription": (pr["body"] or "").replace("\n", " ").replace(",", " "),
        "prStatus": "closed" if pr["state"] == "closed" else "open",
        "prFilePaths": get_pr_file_paths(pr["number"]),
        "prCreatorLoginId": pr["user"]["login"],
        "prCreatedDate": pr["created_at"],
        "prMergedDate": pr["closed_at"] if pr["state"] == "closed" else "N/A"
    }

def get_pr_number_from_commit(commit_id):
    """Fetch PR number from commit ID (if it exists)."""
    url = f"{BASE_URL}/commits/{commit_id}/pulls"
    response = requests.get(url, headers=HEADERS)

    if response.status_code == 200 and response.json():
        return response.json()[0]["number"]  # Get the first associated PR

    return None

def fetch_pr_details(issue_id, pr_number):
    """Fetch full PR details when only the PR number is available."""
    url = f"{BASE_URL}/pulls/{pr_number}"
    response = requests.get(url, headers=HEADERS)

    if response.status_code == 200:
        pr = response.json()
        return extract_pr_details(issue_id, pr)

    return None

def get_pr_file_paths(pr_number):
    """Fetch file paths changed in a pull request."""
    url = f"{BASE_URL}/pulls/{pr_number}/files"
    response = requests.get(url, headers=HEADERS)

    if response.status_code != 200:
        print(f"Error fetching files for PR #{pr_number}")
        return "N/A"

    return "; ".join([file["filename"] for file in response.json()])

def save_issue_to_csv(issue):
    """Save a single issue record to CSV."""
    with open(ISSUE_CSV, "a", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow([issue["id"], issue["creator"], issue["created_at"], issue["closed_at"], issue["title"], issue["body"]])

def save_pr_to_csv(pr):
    """Save a single PR record to CSV."""
    with open(PR_CSV, "a", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow([pr["issueId"], pr["prId"], pr["prTitle"], pr["prDescription"], pr["prStatus"], pr["prFilePaths"], pr["prCreatorLoginId"], pr["prCreatedDate"], pr["prMergedDate"]])

def main():
    initialize_csv()
    print(f"Fetching closed issues from the last {FETCH_MONTHS} months...")

    issues = get_closed_issues()

    for issue in issues:
        print(f"Processing Issue #{issue['id']} - {issue['title']}")
        linked_prs = get_linked_pull_requests(issue["id"])
        issue["linked_pull_requests"] = linked_prs if linked_prs else []

    print("Finished fetching issues and related PRs.")

if __name__ == "__main__":
    main()
