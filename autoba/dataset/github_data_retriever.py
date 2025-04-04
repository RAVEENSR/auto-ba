import requests
import time
from datetime import datetime, timedelta
import csv
import os
import json
import logging
import sys

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("github_fetch.log"),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

# GitHub API Configuration
GITHUB_TOKEN = os.getenv("GITHUB_PAT")
if not GITHUB_TOKEN:
    raise ValueError("GitHub PAT is not set. Please set the GITHUB_PAT environment variable.")

REPO_OWNER = os.getenv("REPO_OWNER")
if not REPO_OWNER:
    raise ValueError("REPO_OWNER is not set. Please set the REPO_OWNER environment variable.")

REPO_NAME = os.getenv("REPO_NAME")
if not REPO_NAME:
    raise ValueError("REPO_NAME is not set. Please set the REPO_NAME environment variable.")

BASE_URL = f"https://api.github.com/repos/{REPO_OWNER}/{REPO_NAME}"
HEADERS = {
    "Authorization": f"token {GITHUB_TOKEN}",
    "Accept": "application/vnd.github.v3+json"
}

# Fetch months from environment variable, default to 4 if not set
FETCH_MONTHS = int(os.getenv("FETCH_MONTHS", 4))
FETCH_START_DATE = datetime.now() - timedelta(days=FETCH_MONTHS * 30)

# CSV Filenames
ISSUE_CSV = f"{REPO_NAME}_issue_details.csv"
PR_CSV = f"{REPO_NAME}_related_pr_details.csv"
STATE_FILE = f"{REPO_NAME}_fetch_state.json"


# Ensure CSV files exist with headers
def initialize_csv():
    if not os.path.exists(ISSUE_CSV):
        with open(ISSUE_CSV, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(["issueId", "issueCreatorLoginId", "createdDate", "closedDate", "closedBy", "commenters", "title", "description"])

    if not os.path.exists(PR_CSV):
        with open(PR_CSV, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(
                ["issueId", "prId", "prTitle", "prDescription", "prStatus", "prFilePaths", "prCreatorLoginId",
                 "prCreatedDate", "prMergedDate"])


def load_state():
    """Load the current processing state from file."""
    if os.path.exists(STATE_FILE):
        try:
            with open(STATE_FILE, "r") as f:
                return json.load(f)
        except json.JSONDecodeError:
            logger.warning("Corrupted state file. Starting fresh.")

    # Default state
    return {
        "last_issue_id": None,
        "last_closed_at": None,
        "processed_issues": [],
        "processing_issue": None,
        "processing_issue_prs": []
    }


def save_state(state):
    """Save the current processing state to file."""
    with open(STATE_FILE, "w") as f:
        json.dump(state, f)


def get_last_saved_issue_info():
    """Gets the last saved issue's number and closed_at date to resume from that point."""
    state = load_state()

    # If we have state information, use it
    if state["last_issue_id"] and state["last_closed_at"]:
        return state["last_issue_id"], state["last_closed_at"]

    # Otherwise fall back to checking the CSV
    if not os.path.exists(ISSUE_CSV):
        return None, None

    with open(ISSUE_CSV, "r", encoding="utf-8") as f:
        lines = f.readlines()
        if len(lines) < 2:
            return None, None  # No issues saved yet

    last_line = lines[-1].strip().split(",")
    last_issue_id = int(last_line[0])
    last_closed_at = last_line[3]  # Closed date in ISO format

    # Update the state
    state["last_issue_id"] = last_issue_id
    state["last_closed_at"] = last_closed_at
    save_state(state)

    return last_issue_id, last_closed_at


def get_closed_issues():
    """Fetch closed issues from GitHub since the last recorded closed issue date, filtering by FETCH_MONTHS."""
    state = load_state()
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
        try:
            response = requests.get(url, headers=HEADERS)
            response.raise_for_status()
            data = response.json()
            if not isinstance(data, list):  # Ensure we got a list response
                logger.error(f"Unexpected API response: {data}")
                break
        except requests.exceptions.RequestException as e:
            logger.error(f"Error fetching issues page {page}: {e}")
            time.sleep(60)  # Wait a minute before retrying
            continue
        except json.JSONDecodeError:
            logger.error("Failed to decode JSON response from GitHub")
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
                logger.debug(f"Skipping Issue #{issue['number']} - Closed on {closed_at_date}")
                continue

            issue_id = issue["number"]

            # Skip already saved issues
            if last_saved_issue and issue_id <= last_saved_issue:
                continue

            # Skip if we've already processed this issue fully according to our state
            if issue_id in state["processed_issues"]:
                logger.info(f"Issue #{issue_id} already processed completely, skipping")
                continue

            # Handle `closed_by` safely
            closed_by_user = issue.get("closed_by")
            closed_by_user = closed_by_user["login"] if closed_by_user else "N/A"

            # Fetch list of users who commented
            commenters = get_issue_commenters(issue_id)

            issues.append({
                "id": issue_id,
                "creator": issue["user"]["login"],
                "created_at": issue["created_at"],
                "closed_at": closed_at,
                "closed_by": closed_by_user,
                "commenters": commenters,
                "title": issue["title"].replace("\n", " ").replace(",", " "),
                "body": (issue["body"] or "").replace("\n", " ").replace(",", " "),
            })

            # Save the issue details first
            save_issue_to_csv(issues[-1])

            # Update state with last saved issue
            state["last_issue_id"] = issue_id
            state["last_closed_at"] = closed_at
            save_state(state)

        logger.info(f"Fetched {len(data)} issues from page {page}")
        page += 1
        time.sleep(2)

    return issues

def get_issue_commenters(issue_id):
    """Fetch users who have commented on a given issue."""
    url = f"{BASE_URL}/issues/{issue_id}/comments"
    commenters = set()

    try:
        response = requests.get(url, headers=HEADERS)
        response.raise_for_status()
        data = response.json()

        if not isinstance(data, list):  # Ensure we have a list of comments
            logger.error(f"Unexpected API response for issue #{issue_id}: {data}")
            return "N/A"

        for comment in data:
            if comment.get("user"):
                commenters.add(comment["user"].get("login", "Unknown"))

    except requests.exceptions.RequestException as e:
        logger.error(f"Error fetching comments for issue #{issue_id}: {e}")

    return "; ".join(commenters) if commenters else "N/A"


def get_linked_pull_requests(issue_number):
    """Fetch linked pull requests from the same repository for a given issue."""
    state = load_state()
    linked_prs = []

    # Check if we were in the middle of processing this issue
    if state["processing_issue"] == issue_number:
        logger.info(f"Resuming PR fetch for issue #{issue_number}")
        linked_prs = state["processing_issue_prs"]
    else:
        # Start processing this issue
        state["processing_issue"] = issue_number
        state["processing_issue_prs"] = []
        save_state(state)

    try:
        # Step 1: Check timeline for cross-referenced PRs
        timeline_url = f"{BASE_URL}/issues/{issue_number}/timeline"
        try:
            response = requests.get(timeline_url, headers=HEADERS)
            response.raise_for_status()

            for event in response.json():
                if event.get("event") == "cross-referenced" and "source" in event:
                    source = event["source"]
                    if "issue" in source and "pull_request" in source["issue"]:
                        pr = source["issue"]
                        pr_number = pr["number"]

                        # Check if we've already processed this PR for this issue
                        pr_already_processed = False
                        for existing_pr in linked_prs:
                            if existing_pr.get("prId") == pr_number:
                                pr_already_processed = True
                                break

                        if not pr_already_processed:
                            pr_data = extract_pr_details(issue_number, pr)
                            if pr_data:
                                linked_prs.append(pr_data)
                                save_pr_to_csv(pr_data)

                                # Update state after each PR is processed
                                state["processing_issue_prs"] = linked_prs
                                save_state(state)

        except requests.exceptions.RequestException as e:
            logger.error(f"Error fetching timeline for issue #{issue_number}: {e}")
            return linked_prs  # Return what we have so far

        # Step 2: Check issue events for referenced PRs
        events_url = f"{BASE_URL}/issues/{issue_number}/events"
        try:
            response = requests.get(events_url, headers=HEADERS)
            response.raise_for_status()

            for event in response.json():
                if event.get("event") == "referenced" and "commit_id" in event:
                    pr_number = get_pr_number_from_commit(event["commit_id"])
                    if pr_number:
                        # Check if we've already processed this PR for this issue
                        pr_already_processed = False
                        for existing_pr in linked_prs:
                            if existing_pr.get("prId") == pr_number:
                                pr_already_processed = True
                                break

                        if not pr_already_processed:
                            pr_data = fetch_pr_details(issue_number, pr_number)
                            if pr_data:
                                linked_prs.append(pr_data)
                                save_pr_to_csv(pr_data)

                                # Update state after each PR is processed
                                state["processing_issue_prs"] = linked_prs
                                save_state(state)

        except requests.exceptions.RequestException as e:
            logger.error(f"Error fetching events for issue #{issue_number}: {e}")
            return linked_prs  # Return what we have so far

    except Exception as e:
        logger.error(f"Unexpected error processing issue #{issue_number}: {e}")
        return linked_prs  # Return what we have so far

    # Mark issue as completely processed
    state["processed_issues"].append(issue_number)
    state["processing_issue"] = None
    state["processing_issue_prs"] = []
    save_state(state)

    return linked_prs


def extract_pr_details(issue_id, pr):
    """Extract PR details from GitHub API response."""
    try:
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
    except Exception as e:
        logger.error(f"Error extracting PR details for PR #{pr['number']}: {e}")
        return None


def get_pr_number_from_commit(commit_id):
    """Fetch PR number from commit ID (if it exists)."""
    url = f"{BASE_URL}/commits/{commit_id}/pulls"
    try:
        response = requests.get(url, headers=HEADERS)
        response.raise_for_status()

        if response.json():
            return response.json()[0]["number"]  # Get the first associated PR
    except requests.exceptions.RequestException as e:
        logger.error(f"Error fetching PR number from commit {commit_id}: {e}")

    return None


def fetch_pr_details(issue_id, pr_number):
    """Fetch full PR details when only the PR number is available."""
    url = f"{BASE_URL}/pulls/{pr_number}"
    try:
        response = requests.get(url, headers=HEADERS)
        response.raise_for_status()

        pr = response.json()
        return extract_pr_details(issue_id, pr)
    except requests.exceptions.RequestException as e:
        logger.error(f"Error fetching details for PR #{pr_number}: {e}")

    return None


def get_pr_file_paths(pr_number):
    """Fetch file paths changed in a pull request."""
    url = f"{BASE_URL}/pulls/{pr_number}/files"
    try:
        response = requests.get(url, headers=HEADERS)
        response.raise_for_status()

        return "; ".join([file["filename"] for file in response.json()])
    except requests.exceptions.RequestException as e:
        logger.error(f"Error fetching files for PR #{pr_number}: {e}")

    return "N/A"


def save_issue_to_csv(issue):
    """Save a single issue record to CSV."""
    try:
        with open(ISSUE_CSV, "a", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(
                [issue["id"], issue["creator"], issue["created_at"], issue["closed_at"], issue["closed_by"], issue["commenters"], issue["title"], issue["body"]])
        logger.debug(f"Saved issue #{issue['id']} to CSV")
    except Exception as e:
        logger.error(f"Error saving issue #{issue['id']} to CSV: {e}")



def save_pr_to_csv(pr):
    """Save a single PR record to CSV."""
    if not pr:
        return

    try:
        with open(PR_CSV, "a", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(
                [pr["issueId"], pr["prId"], pr["prTitle"], pr["prDescription"], pr["prStatus"], pr["prFilePaths"],
                 pr["prCreatorLoginId"], pr["prCreatedDate"], pr["prMergedDate"]])
        logger.debug(f"Saved PR #{pr['prId']} for issue #{pr['issueId']} to CSV")
    except Exception as e:
        logger.error(f"Error saving PR #{pr['prId']} for issue #{pr['issueId']} to CSV: {e}")


def main():
    try:
        initialize_csv()
        logger.info(f"Fetching closed issues from the last {FETCH_MONTHS} months...")

        # Load state
        state = load_state()

        # Check if we were in the middle of processing an issue
        if state["processing_issue"]:
            logger.info(f"Resuming from issue #{state['processing_issue']}")
            get_linked_pull_requests(state["processing_issue"])

        # Continue with normal issue fetching
        issues = get_closed_issues()

        for issue in issues:
            logger.info(f"Processing Issue #{issue['id']} - {issue['title']}")
            linked_prs = get_linked_pull_requests(issue["id"])
            issue["linked_pull_requests"] = linked_prs if linked_prs else []

        logger.info("Finished fetching issues and related PRs.")

    except KeyboardInterrupt:
        logger.info("Process interrupted by user. Progress has been saved.")
    except Exception as e:
        logger.error(f"Unhandled exception: {e}")
        logger.info("The script can be resumed by running it again.")


if __name__ == "__main__":
    main()